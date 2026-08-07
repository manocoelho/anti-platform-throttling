"""Testes de integracao da mensageria: topologia, publicacao, DLQ e fanout.

Dois comportamentos merecem verificacao contra o RabbitMQ real, porque dependem
de detalhes do broker que nao daria para simular com confianca:

1. A FILA DE RETRY DEVOLVE A MENSAGEM PARA A FILA CORRETA. A mensagem vai para
   `apt.retry.youtube.1` com routing key `tier.1.youtube`, espera o TTL e volta
   para `apt.tasks` com `x-dead-letter-routing-key=youtube` -- declarado, nao
   "preservado" (ver TRADE-OFFS.md item 14 para a versao anterior, que
   preservava `tier.1.youtube` em vez da plataforma e perdia a mensagem).

2. O FANOUT ENTREGA A TODAS AS FILAS. E a razao de o exchange de controle ser
   fanout e nao topic: uma invalidacao de feature flag precisa chegar a todos os
   workers, nao a um deles.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import aio_pika
import pytest
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

from apt.config import get_settings
from apt.domain.models import ControlMessage, Platform, SendTaskMessage
from apt.messaging.publisher import Publisher
from apt.messaging.topology import (
    EXCHANGE_CONTROL,
    QUEUE_DLQ,
    declare_topology,
    retry_queue_name,
    task_queue_name,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def channel() -> AsyncIterator[AbstractRobustChannel]:
    """Canal AMQP, com skip automatico se o RabbitMQ nao responder."""
    settings = get_settings()
    connection: AbstractRobustConnection | None = None
    try:
        connection = await asyncio.wait_for(
            aio_pika.connect_robust(settings.rabbitmq_url), timeout=5.0
        )
    except Exception:
        pytest.skip("RabbitMQ indisponivel -- suba o stack com `docker compose up -d`")

    ch = await connection.channel()
    yield ch
    await connection.close()


@pytest.fixture
async def publisher() -> AsyncIterator[Publisher]:
    pub = Publisher()
    try:
        await asyncio.wait_for(pub.connect(), timeout=5.0)
    except Exception:
        pytest.skip("RabbitMQ indisponivel -- suba o stack com `docker compose up -d`")
    yield pub
    await pub.close()


def build_message(**overrides: object) -> SendTaskMessage:
    base: dict[str, object] = {
        "task_id": "11111111-1111-1111-1111-111111111111",
        "campaign_id": "22222222-2222-2222-2222-222222222222",
        "content_id": "33333333-3333-3333-3333-333333333333",
        "platform": Platform.YOUTUBE,
        "content_url": "https://youtube.com/watch?v=msg",
        "correlation_id": "teste-msg",
        "scheduled_at": "2026-08-07T12:00:00+00:00",
    }
    base.update(overrides)
    return SendTaskMessage(**base)  # type: ignore[arg-type]


class TestTopologia:
    async def test_declaracao_e_idempotente(self, channel: AbstractRobustChannel) -> None:
        """Declarar duas vezes nao estoura.

        E o que permite API e worker chamarem `declare_topology()` no boot sem
        coordenacao -- quem chegar primeiro cria, o outro apenas confirma.
        """
        first = await declare_topology(channel)
        second = await declare_topology(channel)
        assert set(first.task_queues) == set(second.task_queues)

    async def test_uma_fila_por_plataforma(self, channel: AbstractRobustChannel) -> None:
        """Filas dedicadas sao a metade estrutural do bulkhead.

        Mil tarefas de Instagram acumuladas nao ficam a frente das de YouTube,
        porque estao em outra fila.
        """
        topology = await declare_topology(channel)
        assert task_queue_name("youtube") in {q.name for q in topology.task_queues.values()}
        assert task_queue_name("instagram") in {q.name for q in topology.task_queues.values()}

    async def test_tres_degraus_de_retry_por_plataforma(
        self, channel: AbstractRobustChannel
    ) -> None:
        """Uma fila por plataforma x degrau -- nao uma so por degrau.

        E o que permite `x-dead-letter-routing-key` apontar para a plataforma
        certa (ver TRADE-OFFS.md item 14).
        """
        topology = await declare_topology(channel)
        assert set(topology.retry_queues) == {
            ("youtube", 1),
            ("youtube", 2),
            ("youtube", 3),
            ("instagram", 1),
            ("instagram", 2),
            ("instagram", 3),
        }
        assert topology.retry_queues[("youtube", 1)].name == retry_queue_name("youtube", 1)

    async def test_dlq_declarada(self, channel: AbstractRobustChannel) -> None:
        topology = await declare_topology(channel)
        assert topology.dlq.name == QUEUE_DLQ


class TestPublicacao:
    async def test_tarefa_chega_na_fila_da_plataforma(
        self, channel: AbstractRobustChannel, publisher: Publisher
    ) -> None:
        """A routing key e a plataforma; o topic exchange faz o resto."""
        topology = await declare_topology(channel)
        queue = topology.task_queues[str(Platform.INSTAGRAM)]
        await queue.purge()

        message = build_message(platform=Platform.INSTAGRAM)
        await publisher.publish_task(message)

        received = await asyncio.wait_for(queue.get(no_ack=True), timeout=5.0)
        payload = json.loads(received.body)
        assert payload["platform"] == "instagram"
        assert payload["task_id"] == message.task_id

    async def test_mensagem_e_persistente(
        self, channel: AbstractRobustChannel, publisher: Publisher
    ) -> None:
        """`delivery_mode=PERSISTENT` faz o broker gravar a mensagem em disco.

        Sem isso, um `docker compose restart rabbitmq` durante a demo apagaria as
        tarefas em voo sem deixar rastro.
        """
        topology = await declare_topology(channel)
        queue = topology.task_queues[str(Platform.YOUTUBE)]
        await queue.purge()

        await publisher.publish_task(build_message())
        received = await asyncio.wait_for(queue.get(no_ack=True), timeout=5.0)
        assert received.delivery_mode == aio_pika.DeliveryMode.PERSISTENT

    async def test_dead_vai_para_a_dlq(
        self, channel: AbstractRobustChannel, publisher: Publisher
    ) -> None:
        topology = await declare_topology(channel)
        await topology.dlq.purge()

        message = build_message(attempt=4)
        await publisher.publish_dead(message, reason="max_attempts")

        received = await asyncio.wait_for(topology.dlq.get(no_ack=True), timeout=5.0)
        payload = json.loads(received.body)
        assert payload["task_id"] == message.task_id
        assert received.headers.get("x-apt-dead-reason") == "max_attempts"


class TestRetryComTTL:
    async def test_retry_volta_para_a_fila_original(
        self, channel: AbstractRobustChannel, publisher: Publisher
    ) -> None:
        """A mensagem espera o TTL na fila de retry e volta para a fila da plataforma.

        ESTE E O TESTE QUE VERIFICA O MECANISMO DE BACKOFF SEM BLOQUEAR WORKER.

        O caminho completo: publish em `apt.retry` com routing key
        `tier.1.youtube` -> fila `apt.retry.youtube.1` (TTL 1s, sem
        consumidor) -> TTL expira -> o RabbitMQ manda para o DLX daquela fila,
        que e `apt.tasks`, com `x-dead-letter-routing-key=youtube` (declarado
        na fila, nao preservado da entrada) -> a mensagem cai em
        `apt.tasks.youtube`.
        """
        topology = await declare_topology(channel)
        task_queue = topology.task_queues[str(Platform.YOUTUBE)]
        await task_queue.purge()
        await topology.retry_queues[("youtube", 1)].purge()

        message = build_message(attempt=1)
        await publisher.publish_retry(message, tier=1, reason="throttled")

        # Imediatamente apos publicar, a mensagem esta na fila de ESPERA.
        assert await asyncio.wait_for(
            self._wait_for_message_count(topology.retry_queues[("youtube", 1)], expected=1),
            timeout=5.0,
        )

        # Depois do TTL (1s) + margem, ela reaparece na fila da plataforma.
        received = await asyncio.wait_for(
            self._poll_queue(task_queue, timeout_seconds=8.0), timeout=10.0
        )
        assert received is not None, (
            "a mensagem nao voltou para apt.tasks.youtube apos o TTL. "
            "Verifique o x-dead-letter-exchange da fila de retry."
        )
        payload = json.loads(received.body)
        assert payload["task_id"] == message.task_id
        assert payload["attempt"] == 1

    async def test_degrau_fora_da_faixa_e_limitado(
        self, channel: AbstractRobustChannel, publisher: Publisher
    ) -> None:
        """Degrau 99 e limitado ao ultimo existente, em vez de estourar.

        Melhor demorar mais que perder a tarefa por um indice invalido vindo de um
        payload corrompido.
        """
        topology = await declare_topology(channel)
        await topology.retry_queues[("youtube", 3)].purge()

        await publisher.publish_retry(build_message(), tier=99, reason="teste")
        assert await asyncio.wait_for(
            self._wait_for_message_count(topology.retry_queues[("youtube", 3)], expected=1),
            timeout=5.0,
        )

    @staticmethod
    async def _wait_for_message_count(queue: object, *, expected: int) -> bool:
        """Espera a fila declarar `expected` mensagens.

        Consulta por um canal NOVO, nao pelo `queue.channel` que fez a
        declaracao original -- reusar esse canal faz o aio_pika devolver a
        contagem em cache do momento da declaracao (aqui, sempre 0, porque o
        `purge()` roda logo depois), nunca a contagem real vigente no
        servidor. Bug confirmado no cliente (aio_pika/aiormq), nao no
        RabbitMQ: `rabbitmqctl list_queues` e a API de management sempre
        mostraram a contagem correta no mesmo instante em que esta consulta,
        pelo canal antigo, insistia em devolver zero.
        """
        connection = await aio_pika.connect_robust(get_settings().rabbitmq_url)
        try:
            channel = await connection.channel()
            for _ in range(50):
                declared = await channel.declare_queue(
                    queue.name,  # type: ignore[attr-defined]
                    passive=True,
                )
                if declared.declaration_result.message_count >= expected:
                    return True
                await asyncio.sleep(0.1)
            return False
        finally:
            await connection.close()

    @staticmethod
    async def _poll_queue(queue: object, *, timeout_seconds: float) -> object | None:
        """Tenta consumir da fila repetidamente ate o timeout.

        Mesmo motivo do canal novo em `_wait_for_message_count`: `queue` foi
        obtida do canal que declarou e purgou a fila originalmente, e
        reusar esse canal faz `get()` nunca encontrar a mensagem -- mesmo
        com ela genuinamente presente no servidor (confirmado via
        `rabbitmqctl`). Um canal novo, numa fila redeclarada passivamente,
        nao tem esse problema.
        """
        connection = await aio_pika.connect_robust(get_settings().rabbitmq_url)
        try:
            channel = await connection.channel()
            fresh_queue = await channel.declare_queue(
                queue.name,  # type: ignore[attr-defined]
                passive=True,
            )
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout_seconds
            while loop.time() < deadline:
                try:
                    message = await fresh_queue.get(no_ack=True, fail=False)
                    if message is not None:
                        return message
                except Exception:
                    pass
                await asyncio.sleep(0.2)
            return None
        finally:
            await connection.close()


class TestFanoutDeControle:
    async def test_todas_as_filas_recebem_o_evento(
        self, channel: AbstractRobustChannel, publisher: Publisher
    ) -> None:
        """Duas filas ligadas ao fanout recebem A MESMA mensagem.

        ESTE TESTE JUSTIFICA A ESCOLHA DO FANOUT.

        Com um exchange topic e uma fila compartilhada, o RabbitMQ entregaria a
        mensagem a UM consumidor -- e uma invalidacao de feature flag chegaria a um
        worker so, deixando os outros com cache velho por ate 2 segundos.

        As duas filas aqui simulam duas replicas de worker.
        """
        control = await channel.declare_exchange(
            EXCHANGE_CONTROL, aio_pika.ExchangeType.FANOUT, durable=True
        )
        queue_a = await channel.declare_queue("", exclusive=True, auto_delete=True)
        queue_b = await channel.declare_queue("", exclusive=True, auto_delete=True)
        await queue_a.bind(control)
        await queue_b.bind(control)

        await publisher.publish_control(
            ControlMessage(type="flags_changed", payload={"flag": "jitter_enabled"})
        )

        msg_a = await asyncio.wait_for(queue_a.get(no_ack=True), timeout=5.0)
        msg_b = await asyncio.wait_for(queue_b.get(no_ack=True), timeout=5.0)

        assert json.loads(msg_a.body)["type"] == "flags_changed"
        assert json.loads(msg_b.body)["type"] == "flags_changed"
