"""Consumo de mensagens com ack manual, prefetch e shutdown limpo.

Aqui vive a metade do padrao Load Balancing que depende de configuracao: com
`prefetch=1`, o RabbitMQ entrega uma mensagem por worker e so manda a proxima
apos o ack. Isso produz distribuicao justa mesmo com workers de velocidades
diferentes.

Com prefetch alto (o padrao do AMQP e ilimitado), o primeiro worker a conectar
puxa todas as mensagens disponiveis para o seu buffer local e as processa em
serie -- enquanto as outras replicas ficam paradas, com a fila vazia. A fila
parece equilibrada no painel, mas nao esta: as mensagens estao empilhadas na
memoria de um worker so.

O outro ponto sensivel deste modulo e o ack MANUAL:

    ack   -> processada com sucesso, o broker pode esquecer
    nack  -> falhou; com requeue=False vai para a DLQ via DLX
    (nada) -> se o worker morrer sem responder, o broker reentrega a outro

Com ack automatico, o broker considera a mensagem entregue no instante em que a
manda pela rede. Um `kill -9` no worker perderia a tarefa em voo. Com ack
manual, ela volta para a fila e outro worker assume -- que e a garantia
at-least-once em que o sistema se baseia.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractRobustConnection,
)

from apt.config import get_settings
from apt.domain.models import ControlMessage, SendTaskMessage
from apt.logging_setup import bind_correlation_id, get_logger
from apt.messaging.topology import Topology, declare_control_queue, declare_topology

logger = get_logger(__name__)

# Um handler recebe a mensagem de dominio e a mensagem AMQP crua (para ler
# headers e decidir ack/nack).
TaskHandler = Callable[[SendTaskMessage, AbstractIncomingMessage], Awaitable[None]]
ControlHandler = Callable[[ControlMessage], Awaitable[None]]


class Consumer:
    """Consome as filas de tarefas e a fila de controle deste processo."""

    def __init__(self, consumer_tag: str) -> None:
        """
        Args:
            consumer_tag: identificador desta replica. Aparece no painel do
                RabbitMQ e no nome da fila de controle privada, o que torna
                possivel ver quais replicas estao conectadas durante a demo.
        """
        self.consumer_tag = consumer_tag
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._topology: Topology | None = None
        # Sinalizado por `request_stop()`; o loop principal espera nele.
        self._stopping = asyncio.Event()
        # Contador de mensagens em processamento. O shutdown espera chegar a
        # zero antes de fechar a conexao, para nao abandonar tarefa em voo.
        self._in_flight = 0

    # -- Ciclo de vida ------------------------------------------------------
    async def connect(self) -> Topology:
        settings = get_settings()
        self._connection = await aio_pika.connect_robust(
            settings.rabbitmq_url,
            client_properties={"connection_name": f"apt-{self.consumer_tag}"},
        )
        channel = await self._connection.channel()
        self._channel = channel

        # O prefetch (QoS) vale para o CANAL. Precisa ser definido antes de
        # comecar a consumir -- depois, o broker ja mandou o que ia mandar.
        await channel.set_qos(prefetch_count=settings.worker_prefetch)

        self._topology = await declare_topology(channel)
        logger.info(
            "consumer.connected",
            consumer_tag=self.consumer_tag,
            prefetch=settings.worker_prefetch,
        )
        return self._topology

    async def start_task_consumers(self, handler: TaskHandler) -> None:
        """Comeca a consumir a fila de cada plataforma.

        Um consumidor por fila -- e a contrapartida em runtime do bulkhead
        estrutural: as filas sao independentes, entao um acumulo no Instagram
        nao interfere no consumo do YouTube.
        """
        topology = self._require_topology()
        for platform, queue in topology.task_queues.items():
            await queue.consume(
                self._wrap_task_handler(handler),
                consumer_tag=f"{self.consumer_tag}.{platform}",
            )
            logger.info("consumer.task_consumer_started", queue=queue.name)

    async def start_control_consumer(self, handler: ControlHandler) -> None:
        """Comeca a consumir a fila privada de controle deste processo."""
        channel = self._require_channel()
        queue = await declare_control_queue(channel, self.consumer_tag)
        await queue.consume(self._wrap_control_handler(handler), no_ack=True)
        logger.info("consumer.control_consumer_started", queue=queue.name)

    async def wait_until_stopped(self) -> None:
        """Bloqueia ate `request_stop()` ser chamado.

        E o que mantem o processo do worker vivo: o consumo acontece em
        callbacks do event loop, entao o `main` precisa apenas esperar aqui.
        """
        await self._stopping.wait()

    def request_stop(self) -> None:
        """Sinaliza o desligamento (chamado pelo handler de SIGTERM/SIGINT)."""
        logger.info("consumer.stop_requested", consumer_tag=self.consumer_tag)
        self._stopping.set()

    async def close(self, *, drain_timeout: float = 10.0) -> None:
        """Fecha a conexao depois de esperar as tarefas em voo.

        Sem essa espera, fechar a conexao no meio de um envio faria o broker
        reentregar a mensagem -- e a plataforma receberia o envio duas vezes,
        uma pelo worker que morreu no meio e outra pelo que assumiu.

        `drain_timeout` limita a espera: se uma tarefa travou, o processo ainda
        precisa terminar (o Docker manda SIGKILL depois de ~10s de qualquer
        forma).
        """
        deadline = asyncio.get_running_loop().time() + drain_timeout
        while self._in_flight > 0 and asyncio.get_running_loop().time() < deadline:
            logger.info("consumer.draining", in_flight=self._in_flight)
            await asyncio.sleep(0.2)

        if self._in_flight > 0:
            logger.warning(
                "consumer.drain_timeout",
                in_flight=self._in_flight,
                note="mensagens em voo serao reentregues pelo broker",
            )

        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        logger.info("consumer.closed", consumer_tag=self.consumer_tag)

    # -- Wrappers -----------------------------------------------------------
    def _wrap_task_handler(
        self, handler: TaskHandler
    ) -> Callable[[AbstractIncomingMessage], Awaitable[None]]:
        """Envolve o handler de tarefas com parsing, contexto e rede de seguranca."""

        async def _on_message(raw: AbstractIncomingMessage) -> None:
            self._in_flight += 1
            try:
                try:
                    payload = json.loads(raw.body)
                    message = SendTaskMessage.from_dict(payload)
                except Exception as exc:
                    # Mensagem malformada: nunca vai ser processavel, entao
                    # retentar seria loop infinito. Vai direto para a DLQ.
                    logger.error(
                        "consumer.malformed_message",
                        error=str(exc),
                        body=raw.body[:200].decode("utf-8", errors="replace"),
                    )
                    await raw.nack(requeue=False)
                    return

                # Restaura o id de correlacao vindo do produtor: os logs do
                # worker passam a ser correlacionaveis com os da API.
                bind_correlation_id(message.correlation_id or None)

                try:
                    await handler(message, raw)
                except Exception as exc:
                    # Rede de seguranca. Se o handler estourar sem tratar, a
                    # mensagem NAO pode ficar sem resposta: sem ack nem nack,
                    # ela ficaria "unacked" no broker ate a conexao cair,
                    # travando o slot de prefetch desse worker.
                    logger.exception(
                        "consumer.handler_failed",
                        task_id=message.task_id,
                        error=str(exc),
                    )
                    if not raw.processed:
                        await raw.nack(requeue=False)
            finally:
                self._in_flight -= 1

        return _on_message

    def _wrap_control_handler(
        self, handler: ControlHandler
    ) -> Callable[[AbstractIncomingMessage], Awaitable[None]]:
        """Envolve o handler de controle.

        Mensagens de controle usam `no_ack=True`: sao avisos idempotentes
        ("limpe o cache de flags"). Perder um numa queda e aceitavel -- o cache
        expira sozinho por TTL -- e nao vale o custo de rastrear acks.
        """

        async def _on_control(raw: AbstractIncomingMessage) -> None:
            try:
                message = ControlMessage.from_dict(json.loads(raw.body))
            except Exception as exc:
                logger.error("consumer.malformed_control", error=str(exc))
                return
            try:
                await handler(message)
            except Exception as exc:
                logger.exception("consumer.control_handler_failed", error=str(exc))

        return _on_control

    # -- Helpers ------------------------------------------------------------
    def _require_channel(self) -> AbstractChannel:
        if self._channel is None:
            raise RuntimeError("Consumer.connect() precisa ser chamado primeiro")
        return self._channel

    def _require_topology(self) -> Topology:
        if self._topology is None:
            raise RuntimeError("Consumer.connect() precisa ser chamado primeiro")
        return self._topology

    @property
    def in_flight(self) -> int:
        """Quantas mensagens estao em processamento agora."""
        return self._in_flight


def read_header_int(message: AbstractIncomingMessage, key: str, default: int = 0) -> int:
    """Le um header inteiro da mensagem AMQP com tolerancia a tipo.

    Os headers vem do broker como `int`, `str` ou `bytes` dependendo do cliente
    que publicou. Normalizar aqui evita repetir o try/except em cada uso.
    """
    raw: Any = (message.headers or {}).get(key, default)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default