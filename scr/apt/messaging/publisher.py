"""Publicacao de mensagens no RabbitMQ.

Um `Publisher` por processo, guardando conexao e canal. Publicar abrindo
conexao nova a cada mensagem custaria um handshake TCP + AMQP (dezenas de ms)
por tarefa -- inviavel para o dispatcher, que publica centenas por segundo.

Tres garantias implementadas aqui:

1. `DeliveryMode.PERSISTENT` -- a mensagem e gravada em disco pelo broker.
2. Publisher confirms -- `aio_pika` com `publisher_confirms=True` faz `publish()`
   somente retornar depois que o broker confirmou o recebimento. Sem isso,
   `publish()` retorna assim que a mensagem entra no buffer TCP local, e uma
   queda do broker nesse instante perde a tarefa silenciosamente.
3. Reconexao automatica -- `connect_robust` reconecta e redeclara a topologia
   sozinho quando o broker volta.
"""

from __future__ import annotations

import json
from typing import Any

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from apt.config import get_settings
from apt.domain.models import ControlMessage, Platform, SendTaskMessage
from apt.logging_setup import get_correlation_id, get_logger
from apt.messaging.topology import (
    RETRY_TIERS_MS,
    Topology,
    declare_topology,
    retry_routing_key,
)

logger = get_logger(__name__)


class Publisher:
    """Publica tarefas, retries e eventos de controle."""

    def __init__(self) -> None:
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._topology: Topology | None = None

    # -- Ciclo de vida ------------------------------------------------------
    async def connect(self) -> None:
        """Abre conexao, canal e declara a topologia. Idempotente."""
        if self._connection is not None and not self._connection.is_closed:
            return

        settings = get_settings()
        # connect_robust (e nao connect): a biblioteca reconecta sozinha em caso
        # de queda e redeclara exchanges/filas. Com `connect`, uma queda de dois
        # segundos do broker exigiria reiniciar o processo.
        self._connection = await aio_pika.connect_robust(
            settings.rabbitmq_url,
            client_properties={"connection_name": f"apt-{settings.service_name}-pub"},
        )
        channel = await self._connection.channel(publisher_confirms=True)
        self._channel = channel
        self._topology = await declare_topology(channel)
        logger.info("publisher.connected")

    async def close(self) -> None:
        """Fecha canal e conexao. Chamado no shutdown."""
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
            logger.info("publisher.closed")
        self._connection = None
        self._channel = None
        self._topology = None

    def _require_topology(self) -> Topology:
        if self._topology is None:
            raise RuntimeError("Publisher.connect() precisa ser chamado antes de publicar")
        return self._topology

    # -- Publicacao ---------------------------------------------------------
    async def publish_task(self, message: SendTaskMessage) -> None:
        """Publica uma tarefa de envio na fila da sua plataforma.

        A routing key e o nome da plataforma, e o exchange topic a encaminha
        para `apt.tasks.<plataforma>`.
        """
        topology = self._require_topology()
        await topology.tasks.publish(
            self._build_message(message.to_dict(), correlation_id=message.correlation_id),
            routing_key=str(message.platform),
        )
        logger.debug(
            "publisher.task_published",
            task_id=message.task_id,
            platform=str(message.platform),
            attempt=message.attempt,
        )

    async def publish_retry(self, message: SendTaskMessage, *, tier: int, reason: str) -> None:
        """Reenfileira a tarefa numa fila de retry com atraso.

        A mensagem espera o TTL da fila e volta sozinha para `apt.tasks`, sem
        ocupar worker durante a espera (ver docstring de `topology.py`).

        Args:
            message: a tarefa, ja com `attempt` incrementado.
            tier: degrau de backoff (1..3). Valores fora da faixa sao limitados
                ao ultimo degrau -- e melhor demorar mais que estourar.
            reason: por que houve retry. Vai para o header e para o log, e e o
                que permite distinguir depois um retry por 429 de um por 5xx.
        """
        topology = self._require_topology()
        clamped = max(1, min(tier, len(RETRY_TIERS_MS)))
        platform_str = str(message.platform)

        await topology.retry.publish(
            self._build_message(
                message.to_dict(),
                correlation_id=message.correlation_id,
                headers={
                    "x-apt-retry-reason": reason,
                    "x-apt-retry-tier": clamped,
                    "x-apt-attempt": message.attempt,
                },
            ),
            # A routing key aqui seleciona a FILA DE ESPERA desta plataforma.
            # Ao expirar o TTL, a fila de retry manda a mensagem para
            # apt.tasks com x-dead-letter-routing-key = a propria plataforma
            # (declarado em topology.py, nao mais "preservado" da entrada).
            routing_key=retry_routing_key(platform_str, clamped),
        )
        logger.info(
            "publisher.retry_scheduled",
            task_id=message.task_id,
            platform=str(message.platform),
            attempt=message.attempt,
            tier=clamped,
            delay_ms=RETRY_TIERS_MS[clamped - 1],
            reason=reason,
        )

    async def publish_dead(self, message: SendTaskMessage, *, reason: str) -> None:
        """Manda a tarefa direto para a DLQ, sem mais tentativas."""
        topology = self._require_topology()
        await topology.dlx.publish(
            self._build_message(
                message.to_dict(),
                correlation_id=message.correlation_id,
                headers={
                    "x-apt-dead-reason": reason,
                    "x-apt-attempt": message.attempt,
                },
            ),
            routing_key=str(message.platform),
        )
        logger.warning(
            "publisher.task_dead",
            task_id=message.task_id,
            platform=str(message.platform),
            attempts=message.attempt,
            reason=reason,
        )

    async def publish_control(self, message: ControlMessage) -> None:
        """Difunde um evento de controle para todos os workers (fanout)."""
        topology = self._require_topology()
        await topology.control.publish(
            self._build_message(message.to_dict()),
            # Fanout ignora a routing key; passamos string vazia por clareza.
            routing_key="",
        )
        logger.info("publisher.control_published", type=message.type)

    # -- Helpers ------------------------------------------------------------
    @staticmethod
    def _build_message(
        payload: dict[str, Any],
        *,
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> aio_pika.Message:
        """Monta a mensagem AMQP com as propriedades padrao do projeto."""
        return aio_pika.Message(
            body=json.dumps(payload, separators=(",", ":")).encode(),
            content_type="application/json",
            # PERSISTENT faz o broker gravar em disco. Custa latencia de
            # publicacao e e o preco de nao perder tarefas num restart.
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            correlation_id=correlation_id or get_correlation_id() or None,
            headers=headers or {},
        )


# ---------------------------------------------------------------------------
# Instancia compartilhada do processo
# ---------------------------------------------------------------------------
_publisher: Publisher | None = None


def get_publisher() -> Publisher:
    """Devolve o publisher do processo (criado na primeira chamada).

    O objeto e criado ja aqui, mas `connect()` continua sendo responsabilidade
    de quem sobe o servico -- assim o lifespan da API controla explicitamente
    quando a conexao com o broker e aberta e fechada.
    """
    global _publisher
    if _publisher is None:
        _publisher = Publisher()
    return _publisher


async def close_publisher() -> None:
    """Fecha o publisher do processo, se existir."""
    global _publisher
    if _publisher is not None:
        await _publisher.close()
        _publisher = None


def platform_routing_key(platform: Platform) -> str:
    """Routing key de uma plataforma. Existe para documentar a convencao."""
    return str(platform)
