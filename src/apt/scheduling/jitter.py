"""Distribuicao temporal dos envios -- o "jitter variavel" do escopo da POC 4.

O PROBLEMA QUE ESTE MODULO RESOLVE

Respeitar o limite de vazao nao basta. Um sistema que envia exatamente 16
requisicoes no primeiro milissegundo de cada segundo respeita 16 req/s e ainda
assim exibe um padrao obviamente automatizado: intervalos identicos, rajadas
alinhadas ao relogio, variancia zero.

Os mecanismos de deteccao das plataformas nao olham so o volume; olham a FORMA
da distribuicao. Regularidade perfeita e assinatura de maquina.

Este modulo espalha os envios dentro de cada intervalo para que a serie temporal
tenha a irregularidade de trafego organico.

AS TRES ESTRATEGIAS

UNIFORM
    Sorteio uniforme em torno do intervalo medio. Simples, com variancia
    limitada. Boa quando o objetivo e apenas nao ser periodico.

EXPONENTIAL
    Intervalos com distribuicao exponencial -- o que caracteriza um processo de
    Poisson. E o modelo estatistico padrao para chegadas independentes: se N
    pessoas decidem interagir com um conteudo sem se coordenar, os intervalos
    entre as interacoes seguem justamente essa distribuicao. E a estrategia
    estatisticamente mais defensavel.

HUMANIZED (padrao)
    Exponencial modulada por um perfil de atividade ao longo do dia. Alem dos
    intervalos parecerem organicos, o VOLUME acompanha o ritmo humano: cai de
    madrugada, sobe no fim da tarde. Um volume perfeitamente constante ao longo
    de 24 horas e, por si so, um sinal artificial -- nenhuma audiencia real se
    comporta assim.

TUDO AQUI E PURO E DETERMINISTICO SOB `Random` INJETADO

As funcoes recebem opcionalmente uma instancia de `random.Random`. Isso permite
que os testes fixem a semente e verifiquem propriedades estatisticas (media
dentro da tolerancia, nenhum valor negativo, monotonicidade do perfil diario)
sem depender de sorte.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

from apt.domain.models import JitterStrategy

# Gerador padrao, usado quando nenhum `Random` e injetado. Uma INSTANCIA e nao
# o modulo `random`: o modulo tem a mesma API, mas o tipo dele nao e
# `random.Random` e isso deixaria a anotacao das funcoes imprecisa.
_DEFAULT_RNG = random.Random()

# Perfil de atividade por hora do dia (0-23), em UTC. Valores relativos: 1.0 e a
# atividade de referencia. Nao pretendem modelar nenhuma audiencia real -- sao
# uma curva plausivel, com vale de madrugada e pico no inicio da noite, para
# demonstrar o mecanismo de modulacao.
HOURLY_ACTIVITY_PROFILE: tuple[float, ...] = (
    0.25,
    0.18,
    0.14,
    0.12,
    0.13,
    0.20,  # 00h-05h: vale
    0.40,
    0.65,
    0.85,
    0.95,
    1.00,
    1.05,  # 06h-11h: subida
    1.10,
    1.05,
    1.00,
    1.05,
    1.15,
    1.30,  # 12h-17h: platô
    1.45,
    1.50,
    1.40,
    1.10,
    0.75,
    0.45,  # 18h-23h: pico e queda
)

# Piso do multiplicador. Sem ele, uma hora de atividade 0.12 faria o intervalo
# entre envios crescer 8x e a campanha praticamente parar de madrugada -- o que
# atrasaria o orcamento total de forma que nao daria para compensar depois.
_MIN_ACTIVITY = 0.15


@dataclass(frozen=True, slots=True)
class JitterPlan:
    """Resultado de um planejamento de tick.

    Attributes:
        count: quantas tarefas materializar neste tick.
        offsets_ms: deslocamento de cada tarefa dentro do tick, em ms. Vira o
            `scheduled_at` da tarefa e e o que registra a intencao de
            espalhamento -- comparar `scheduled_at` com o instante real do envio
            mede o atraso que o rate limiter introduziu.
        mean_interval_ms: intervalo medio pretendido, para log e diagnostico.
    """

    count: int
    offsets_ms: tuple[int, ...]
    mean_interval_ms: float


def activity_multiplier(hour_utc: int) -> float:
    """Multiplicador de atividade da hora informada (0-23).

    Raises:
        ValueError: se a hora estiver fora da faixa. Falhar alto porque um
            indice errado silenciosamente pegaria o perfil de outra hora e
            distorceria a distribuicao sem nenhum sinal visivel.
    """
    if not 0 <= hour_utc <= 23:
        raise ValueError(f"hora precisa estar entre 0 e 23, recebido {hour_utc}")
    return max(_MIN_ACTIVITY, HOURLY_ACTIVITY_PROFILE[hour_utc])


def uniform_offsets(
    count: int, *, window_ms: int, rng: random.Random | None = None
) -> tuple[int, ...]:
    """`count` deslocamentos uniformes dentro de uma janela de `window_ms`.

    Os offsets voltam ORDENADOS. Nao e cosmetico: o dispatcher publica na ordem
    em que os recebe, e publicar fora de ordem faria uma tarefa com
    `scheduled_at` posterior chegar a fila antes de outra anterior -- os numeros
    de atraso no relatorio ficariam sem sentido.
    """
    r = rng if rng is not None else _DEFAULT_RNG
    if count <= 0:
        return ()
    return tuple(sorted(int(r.uniform(0, window_ms)) for _ in range(count)))


def exponential_offsets(
    count: int, *, mean_interval_ms: float, rng: random.Random | None = None
) -> tuple[int, ...]:
    """Deslocamentos acumulando intervalos exponenciais (processo de Poisson).

    `expovariate(lambda)` devolve amostras com media `1/lambda`. Passamos
    `lambda = 1 / mean_interval_ms` para que a media dos intervalos seja
    exatamente `mean_interval_ms`.

    Os offsets sao a soma acumulada dos intervalos -- por construcao ja saem
    ordenados e podem ultrapassar a janela do tick. Isso e aceitavel e
    desejavel: um intervalo longo sorteado significa "esse envio sai um pouco
    depois", e o dispatcher trata o excedente como parte do proximo tick.
    """
    r = rng if rng is not None else _DEFAULT_RNG
    if count <= 0:
        return ()
    if mean_interval_ms <= 0:
        raise ValueError(f"mean_interval_ms precisa ser positivo, recebido {mean_interval_ms}")

    lambd = 1.0 / mean_interval_ms
    offsets: list[int] = []
    cursor = 0.0
    for _ in range(count):
        cursor += r.expovariate(lambd)
        offsets.append(int(cursor))
    return tuple(offsets)


def plan_tick(
    *,
    strategy: JitterStrategy,
    target_rate_per_min: float,
    tick_seconds: float,
    hour_utc: int,
    remaining_budget: int,
    max_batch: int,
    jitter_enabled: bool = True,
    rng: random.Random | None = None,
) -> JitterPlan:
    """Planeja quantas tarefas materializar neste tick e como espalha-las.

    Args:
        strategy: estrategia de distribuicao da campanha.
        target_rate_per_min: vazao alvo da campanha.
        tick_seconds: duracao do tick do dispatcher.
        hour_utc: hora atual em UTC (usada pela estrategia HUMANIZED).
        remaining_budget: quantos envios a campanha ainda pode fazer.
        max_batch: teto de seguranca por tick.
        jitter_enabled: quando False, produz o comportamento SEM distribuicao --
            todas as tarefas com offset zero, saindo em rajada. E o modo
            controlado pela feature flag `jitter_enabled`, usado na demonstracao
            para provocar 429 de proposito e mostrar o contraste.
        rng: gerador injetavel, para testes deterministicos.

    Returns:
        O plano do tick.
    """
    r = rng if rng is not None else _DEFAULT_RNG

    if remaining_budget <= 0 or target_rate_per_min <= 0 or tick_seconds <= 0:
        return JitterPlan(count=0, offsets_ms=(), mean_interval_ms=0.0)

    window_ms = int(tick_seconds * 1000)
    # Quantas tarefas o ritmo alvo pede neste tick.
    base_count = target_rate_per_min * (tick_seconds / 60.0)

    if strategy is JitterStrategy.HUMANIZED:
        base_count *= activity_multiplier(hour_utc)

    # `base_count` costuma ser fracionario (ex.: 0.7 tarefas por tick). Truncar
    # levaria a zero para sempre e a campanha nunca sairia; arredondar sempre
    # para cima entregaria mais que o alvo. Tratamos a parte fracionaria como
    # PROBABILIDADE: 0.7 -> 70% de chance de uma tarefa extra. Ao longo de muitos
    # ticks, a media converge para o valor exato.
    whole = int(base_count)
    fraction = base_count - whole
    count = whole + (1 if (jitter_enabled and r.random() < fraction) else 0)
    if not jitter_enabled:
        # Sem jitter, arredondamos deterministicamente para cima -- reforca o
        # comportamento de rajada que a demonstracao quer evidenciar.
        count = math.ceil(base_count)

    count = max(0, min(count, remaining_budget, max_batch))
    if count == 0:
        return JitterPlan(count=0, offsets_ms=(), mean_interval_ms=0.0)

    mean_interval_ms = window_ms / count

    if not jitter_enabled:
        # Modo rajada: tudo no instante zero do tick.
        return JitterPlan(
            count=count,
            offsets_ms=tuple(0 for _ in range(count)),
            mean_interval_ms=0.0,
        )

    match strategy:
        case JitterStrategy.UNIFORM:
            offsets = uniform_offsets(count, window_ms=window_ms, rng=r)
        case JitterStrategy.EXPONENTIAL | JitterStrategy.HUMANIZED:
            offsets = exponential_offsets(count, mean_interval_ms=mean_interval_ms, rng=r)
        case _:  # pragma: no cover - enum exaustivo
            offsets = uniform_offsets(count, window_ms=window_ms, rng=r)

    return JitterPlan(count=count, offsets_ms=offsets, mean_interval_ms=mean_interval_ms)