"""Topologia do RabbitMQ: exchanges, filas, DLX e as filas de retry.

Este modulo e a definicao unica da topologia. API e worker chamam a mesma
funcao `declare_topology()` no boot, e como as declaracoes AMQP sao
idempotentes, quem chegar primeiro cria e o outro apenas confirma.

    Desenho:

    apt.tasks (topic)
      |-- routing key "youtube"   --> apt.tasks.youtube
      |-- routing key "instagram" --> apt.tasks.instagram
                                        |
                                        | (nack / rejeicao)
                                        v
                                   apt.dlx (topic)
                                        |
                                        v
                                    apt.dlq  <- inspecionavel pela API

    apt.retry (topic)  -- uma fila de TTL por PLATAFORMA x DEGRAU, cada uma
    devolvendo a mensagem para a fila da propria plataforma:
      |-- apt.retry.youtube.1    (TTL  1s)  --> apt.tasks.youtube
      |-- apt.retry.youtube.2    (TTL  5s)  --> apt.tasks.youtube
      |-- apt.retry.youtube.3    (TTL 30s)  --> apt.tasks.youtube
      |-- apt.retry.instagram.1  (TTL  1s)  --> apt.tasks.instagram
      |-- apt.retry.instagram.2  (TTL  5s)  --> apt.tasks.instagram
      |-- apt.retry.instagram.3  (TTL 30s)  --> apt.tasks.instagram

    apt.control (fanout)
      |-- fila exclusiva por worker --> todos recebem toda mensagem


POR QUE FILAS DE RETRY COM TTL, E NAO `sleep()` NO WORKER

A forma ingenua de aplicar backoff e `await asyncio.sleep(delay)` antes de
tentar de novo. O problema: com `prefetch=1`, o worker que dorme 30 segundos
segura o seu unico slot e para de consumir. Cinco workers com cinco tarefas em
backoff longo travam o sistema inteiro enquanto a fila cresce.

Aqui a mensagem e republicada numa fila que nao tem consumidor nenhum, apenas
`x-message-ttl`. Quando o TTL expira, o RabbitMQ move a mensagem para o
dead-letter exchange configurado -- que aponta de volta para `apt.tasks`. O
tempo passa dentro do broker, e o worker fica livre.


POR QUE TRES FILAS FIXAS, E NAO TTL POR MENSAGEM

O AMQP permite `expiration` por mensagem, o que daria backoff exponencial
continuo, com jitter exato. Recusamos por causa de um comportamento
documentado do RabbitMQ: a expiracao so e avaliada quando a mensagem chega a
CABECA da fila. Uma mensagem com TTL de 30s publicada antes de outra com TTL de
1s bloqueia a segunda pelos 30 segundos inteiros (head-of-line blocking).

Com tres filas de TTL fixo, toda mensagem numa fila tem o mesmo prazo, e a
ordem FIFO coincide com a ordem de expiracao. O jitter continua existindo -- ele
e aplicado na ESCOLHA da fila e na fila de tier 1 para adiamentos curtos (ver
`resilience/retry.py`). Ver docs/TRADE-OFFS.md.

POR QUE UMA FILA POR PLATAFORMA x DEGRAU, E NAO SO POR DEGRAU

Ate a correcao do TRADE-OFFS.md item 14, existiam 3 filas de retry (uma por
degrau), compartilhadas entre todas as plataformas. Isso era o bug: a routing
key que o RabbitMQ preserva ao dead-letter e a que a mensagem tinha ao ENTRAR
na fila de retry -- `tier.N` -- nao a plataforma que ela tinha antes de
`publish_retry` a republicar. Como `apt.tasks` so tem binding para
`youtube`/`instagram`, nao para `tier.N`, toda mensagem que expirava na fila de
retry chegava inroteavel e era descartada em silencio.

A correcao segrega por plataforma: `apt.retry.<plataforma>.<degrau>`, com
`x-dead-letter-routing-key` explicito. A routing key deixa de ser algo a
"preservar" -- e declarada de proposito, exatamente como a fila de tarefas.
Efeito colateral positivo: o retry passa a ser isolado por plataforma tambem na
fila de espera, reforcando o Bulkhead numa camada que antes nao o tinha (um
retry do Instagram nao compete mais pela mesma fila do que um retry do
YouTube).
"""

from __future__ import annotations

from dataclasses import dataclass

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue

from apt.domain.platforms import all_platforms
from apt.logging_setup import get_logger

logger = get_logger(__name__)

# --- Nomes (constantes para nao haver string literal solta pelo codigo) -----
EXCHANGE_TASKS = "apt.tasks"
EXCHANGE_CONTROL = "apt.control"
EXCHANGE_DLX = "apt.dlx"
EXCHANGE_RETRY = "apt.retry"

QUEUE_DLQ = "apt.dlq"

# Degraus de backoff, em milissegundos. A progressao ~geometrica cobre de
# "engasgo momentaneo" a "plataforma fora do ar" sem precisar de mais filas.
RETRY_TIERS_MS: tuple[int, ...] = (1_000, 5_000, 30_000)


def task_queue_name(platform: str) -> str:
    """Nome da fila dedicada de uma plataforma.

    Uma fila por plataforma e a metade estrutural do padrao Bulkhead: mil
    tarefas de Instagram acumuladas nao ficam a frente das tarefas de YouTube,
    porque estao em outra fila. Numa fila unica compartilhada, a cabeca da fila
    seria um recurso disputado e uma plataforma lenta atrasaria a outra.
    """
    return f"apt.tasks.{platform}"


def retry_queue_name(platform: str, tier: int) -> str:
    """Nome da fila de retry do degrau `tier` (1-indexado) de uma plataforma.

    Uma fila por plataforma x degrau -- nao so por degrau -- e o que permite
    `x-dead-letter-routing-key` apontar para a plataforma certa. Ver a secao
    "POR QUE UMA FILA POR PLATAFORMA x DEGRAU" no docstring do modulo.
    """
    return f"apt.retry.{platform}.{tier}"


def retry_routing_key(platform: str, tier: int) -> str:
    """Routing key da fila de retry do degrau `tier` de uma plataforma."""
    return f"tier.{tier}.{platform}"


@dataclass(slots=True)
class Topology:
    """Referencias vivas aos objetos AMQP declarados.

    Devolvida por `declare_topology()` para que publisher e consumer nao
    precisem redeclarar nem procurar exchanges por nome.
    """

    tasks: AbstractExchange
    control: AbstractExchange
    dlx: AbstractExchange
    retry: AbstractExchange
    task_queues: dict[str, AbstractQueue]
    dlq: AbstractQueue
    # Chave (plataforma, degrau) -- uma fila por combinacao, ver
    # "POR QUE UMA FILA POR PLATAFORMA x DEGRAU" no docstring do modulo.
    retry_queues: dict[tuple[str, int], AbstractQueue]


async def declare_topology(channel: AbstractChannel) -> Topology:
    """Declara toda a topologia. Idempotente e seguro de chamar em paralelo.

    Args:
        channel: canal AMQP aberto.

    Returns:
        As referencias aos objetos declarados.
    """
    # `durable=True` em tudo: exchanges, filas e mensagens sobrevivem a um
    # restart do broker. Sem isso, um `docker compose restart rabbitmq` durante
    # a demo apagaria a fila e as tarefas em voo desapareceriam sem rastro.
    tasks = await channel.declare_exchange(
        EXCHANGE_TASKS, aio_pika.ExchangeType.TOPIC, durable=True
    )
    dlx = await channel.declare_exchange(EXCHANGE_DLX, aio_pika.ExchangeType.TOPIC, durable=True)
    retry = await channel.declare_exchange(
        EXCHANGE_RETRY, aio_pika.ExchangeType.TOPIC, durable=True
    )
    control = await channel.declare_exchange(
        EXCHANGE_CONTROL, aio_pika.ExchangeType.FANOUT, durable=True
    )

    # --- Fila morta ---------------------------------------------------------
    # Sem TTL: uma tarefa que falhou definitivamente fica aqui ate alguem
    # olhar. Expirar automaticamente esconderia o problema.
    dlq = await channel.declare_queue(QUEUE_DLQ, durable=True)
    await dlq.bind(dlx, routing_key="#")

    # --- Uma fila por plataforma (bulkhead estrutural) ---------------------
    task_queues: dict[str, AbstractQueue] = {}
    for platform in all_platforms():
        name = task_queue_name(str(platform))
        queue = await channel.declare_queue(
            name,
            durable=True,
            arguments={
                # Toda rejeicao definitiva cai no DLX, que alimenta a DLQ.
                "x-dead-letter-exchange": EXCHANGE_DLX,
                "x-dead-letter-routing-key": str(platform),
                # Teto de seguranca. Se o consumo parar por muito tempo, o
                # RabbitMQ comeca a descartar as mensagens MAIS ANTIGAS da fila
                # em vez de estourar a memoria do broker e derrubar o cluster
                # inteiro. Perder tarefa antiga e ruim; perder o broker e pior.
                "x-max-length": 100_000,
                "x-overflow": "drop-head",
            },
        )
        await queue.bind(tasks, routing_key=str(platform))
        task_queues[str(platform)] = queue

    # --- Filas de retry (o tempo passa dentro do broker) -------------------
    # Uma fila por PLATAFORMA x DEGRAU -- ver "POR QUE UMA FILA POR PLATAFORMA
    # x DEGRAU" no docstring do modulo. `x-dead-letter-routing-key` explicito
    # e o que corrige o bug documentado no TRADE-OFFS.md item 14: sem ele, o
    # RabbitMQ preservaria a routing key que a mensagem tinha ao ENTRAR na
    # fila de retry ("tier.N.<plataforma>", usada so para rotear ATE aqui),
    # que nao tem binding nenhum em `apt.tasks`.
    retry_queues: dict[tuple[str, int], AbstractQueue] = {}
    for platform in all_platforms():
        platform_str = str(platform)
        for index, ttl_ms in enumerate(RETRY_TIERS_MS, start=1):
            name = retry_queue_name(platform_str, index)
            queue = await channel.declare_queue(
                name,
                durable=True,
                arguments={
                    "x-message-ttl": ttl_ms,
                    # Expirado o TTL, a mensagem vai para o DLX desta fila --
                    # que aqui e o exchange de TAREFAS, nao o de mensagens
                    # mortas. E o que faz a mensagem voltar ao fluxo normal.
                    "x-dead-letter-exchange": EXCHANGE_TASKS,
                    # A routing key de destino e declarada, nao "preservada":
                    # sempre a plataforma desta fila, entregando exatamente em
                    # apt.tasks.<plataforma>.
                    "x-dead-letter-routing-key": platform_str,
                    "x-max-length": 50_000,
                    "x-overflow": "drop-head",
                },
            )
            await queue.bind(retry, routing_key=retry_routing_key(platform_str, index))
            retry_queues[(platform_str, index)] = queue

    logger.info(
        "messaging.topology_declared",
        task_queues=list(task_queues),
        retry_tiers=list(retry_queues),
    )

    return Topology(
        tasks=tasks,
        control=control,
        dlx=dlx,
        retry=retry,
        task_queues=task_queues,
        dlq=dlq,
        retry_queues=retry_queues,
    )


async def declare_control_queue(channel: AbstractChannel, consumer_tag: str) -> AbstractQueue:
    """Cria a fila privada deste processo no exchange fanout de controle.

    O ponto essencial do fanout: cada consumidor precisa da SUA fila. Se todos
    os workers dividissem uma fila unica, o RabbitMQ entregaria cada mensagem de
    controle a apenas um deles -- e uma invalidacao de feature flag chegaria a
    um worker so, deixando os outros com cache velho.

    A fila e `exclusive=True` (some quando a conexao cai) e `auto_delete=True`:
    escalar workers para baixo nao deixa fila orfa acumulando mensagens.

    Args:
        channel: canal AMQP aberto.
        consumer_tag: identificador do processo, para o nome da fila ficar
            legivel no painel do RabbitMQ.
    """
    queue = await channel.declare_queue(
        f"apt.control.{consumer_tag}",
        durable=False,
        exclusive=True,
        auto_delete=True,
    )
    control = await channel.declare_exchange(
        EXCHANGE_CONTROL, aio_pika.ExchangeType.FANOUT, durable=True
    )
    # Fanout ignora routing key -- toda mensagem vai para toda fila ligada.
    await queue.bind(control)
    logger.info("messaging.control_queue_declared", queue=queue.name)
    return queue
