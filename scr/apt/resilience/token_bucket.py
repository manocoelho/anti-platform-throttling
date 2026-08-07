"""Algoritmo do token bucket -- implementacao de referencia, pura e sincrona.

Este modulo nao conhece Redis. E a especificacao executavel do algoritmo:
funcoes puras, deterministicas, sem I/O, testaveis exaustivamente sem subir
nada.

POR QUE UMA VERSAO PURA SE O QUE RODA EM PRODUCAO E O SCRIPT LUA?

O rate limiter real precisa ser atomico entre varios workers, e por isso a
decisao acontece dentro do Redis, num script Lua (`lua/token_bucket.lua`).
Consequencia: o algoritmo existe duas vezes.

Aceitamos essa duplicacao de proposito, por tres razoes:

1. Testabilidade. Testar o Lua exige Redis no ar; testar esta versao nao. Os
   casos de borda -- bucket vazio, refill parcial, relogio para tras, rajada
   maior que a capacidade -- sao cobertos por testes unitarios que rodam em
   milissegundos, sem Docker.
2. Legibilidade. Este arquivo e a explicacao do algoritmo. O Lua e a execucao.
   Quem for entender o mecanismo le Python.
3. Verificacao. `tests/integration/test_rate_limit_e2e.py` roda a MESMA
   sequencia de operacoes nas duas implementacoes e compara os resultados. Se
   alguem alterar uma e esquecer a outra, o teste de paridade quebra.

O trade-off (duas fontes de verdade para o mesmo algoritmo) esta registrado em
docs/TRADE-OFFS.md.

COMO O TOKEN BUCKET FUNCIONA

Um balde tem `capacity` fichas e recebe `refill_rps` fichas por segundo. Cada
requisicao consome uma ficha; sem ficha disponivel, a requisicao e negada e o
chamador recebe quanto tempo falta para a proxima ficha aparecer.

Nao guardamos um temporizador para "pingar" fichas no balde. Guardamos apenas
`(tokens, updated_at)` e calculamos o refill no momento da leitura:

    tokens_agora = min(capacity, tokens + (agora - updated_at) * refill_rps)

Isso torna o estado minimo (dois numeros por balde) e o custo constante -- o que
importa porque este estado vive no Redis e e lido a cada envio.

POR QUE TOKEN BUCKET E NAO SLIDING WINDOW

Uma janela deslizante precisa guardar o timestamp de cada requisicao para saber
quantas ocorreram nos ultimos N segundos: memoria proporcional ao volume, e uma
estrutura que cresce sob carga -- exatamente quando nao se quer isso.

O token bucket usa dois numeros, independentemente do volume, e ainda permite
rajada controlada: um balde cheio absorve `capacity` requisicoes instantaneas e
depois converge para a vazao sustentada. Ver ADR-004.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BucketState:
    """Estado persistido de um balde.

    Attributes:
        tokens: fichas disponiveis. Float porque o refill e continuo -- com
            inteiros, uma vazao de 0.5 req/s truncaria para zero a cada leitura
            e o balde nunca encheria.
        updated_at_ms: instante da ultima atualizacao, em epoch de
            milissegundos. Milissegundos e nao segundos: com vazoes de dezenas
            de req/s, a granularidade de segundo perderia refill demais.
    """

    tokens: float
    updated_at_ms: int


@dataclass(frozen=True, slots=True)
class BucketDecision:
    """Resultado de uma tentativa de consumo.

    Attributes:
        allowed: se a requisicao pode seguir.
        tokens_remaining: fichas restantes apos a decisao.
        retry_after_ms: quando negada, em quanto tempo havera ficha suficiente.
            Zero quando permitida. E este numero que o worker usa para escolher
            o degrau de retry -- sem ele, o worker so poderia adivinhar o
            atraso.
        state: o novo estado, para ser persistido.
    """

    allowed: bool
    tokens_remaining: float
    retry_after_ms: int
    state: BucketState


def refill(state: BucketState, *, capacity: float, refill_rps: float, now_ms: int) -> float:
    """Calcula quantas fichas o balde tem em `now_ms`, sem consumir nada.

    O `max(0, ...)` no tempo decorrido protege contra relogio que anda para
    tras. Isso acontece de verdade: os workers passam `now_ms` a partir do
    proprio relogio, e uma correcao de NTP ou uma diferenca de alguns
    milissegundos entre containers pode produzir um `now_ms` anterior ao
    `updated_at_ms` gravado por outro worker.

    Sem o clamp, `elapsed` negativo REMOVERIA fichas do balde -- e o efeito
    seria um rate limiter mais restritivo do que o configurado, de forma
    intermitente e praticamente impossivel de diagnosticar em producao.
    """
    elapsed_ms = max(0, now_ms - state.updated_at_ms)
    replenished = (elapsed_ms / 1000.0) * refill_rps
    return min(capacity, state.tokens + replenished)


def consume(
    state: BucketState | None,
    *,
    capacity: float,
    refill_rps: float,
    now_ms: int,
    requested: float = 1.0,
) -> BucketDecision:
    """Tenta consumir `requested` fichas.

    Args:
        state: estado atual, ou `None` se o balde nunca existiu. Balde novo
            comeca CHEIO, nao vazio: comecar vazio faria a primeira requisicao
            de um processo recem-iniciado esperar sem nenhuma razao, e um
            deploy passaria a introduzir uma pausa artificial no trafego.
        capacity: fichas maximas (tamanho da rajada tolerada).
        refill_rps: fichas por segundo (vazao sustentada).
        now_ms: instante atual em epoch de milissegundos.
        requested: quantas fichas consumir. Normalmente 1.

    Returns:
        A decisao e o novo estado.

    Raises:
        ValueError: se `capacity` ou `refill_rps` nao forem positivos.
            Validamos porque `refill_rps=0` causaria divisao por zero no
            calculo de `retry_after_ms`, e `capacity=0` negaria tudo para
            sempre -- os dois sao erro de configuracao, nao estado valido.
    """
    if capacity <= 0:
        raise ValueError(f"capacity precisa ser positiva, recebido {capacity}")
    if refill_rps <= 0:
        raise ValueError(f"refill_rps precisa ser positivo, recebido {refill_rps}")

    current = state if state is not None else BucketState(tokens=capacity, updated_at_ms=now_ms)
    available = refill(current, capacity=capacity, refill_rps=refill_rps, now_ms=now_ms)

    # Pedido maior que a capacidade nunca sera atendido, por mais que se espere.
    # Negamos imediatamente com retry_after=0 em vez de devolver um prazo que
    # jamais se cumpriria -- assim o chamador nao entra em espera infinita.
    if requested > capacity:
        return BucketDecision(
            allowed=False,
            tokens_remaining=available,
            retry_after_ms=0,
            state=BucketState(tokens=available, updated_at_ms=now_ms),
        )

    if available >= requested:
        remaining = available - requested
        return BucketDecision(
            allowed=True,
            tokens_remaining=remaining,
            retry_after_ms=0,
            state=BucketState(tokens=remaining, updated_at_ms=now_ms),
        )

    # Negado: calcula quando havera fichas suficientes.
    # Arredondamos para cima -- arredondar para baixo faria o chamador tentar
    # de novo alguns microssegundos antes da ficha existir, e ser negado outra
    # vez. Numa fila movimentada isso vira um ciclo de tentativas inuteis.
    deficit = requested - available
    retry_after_ms = int(deficit / refill_rps * 1000.0) + 1

    # Importante: o estado e gravado com as fichas ATUAIS (nao consumidas) e o
    # timestamp atualizado. Nao "punimos" o balde por uma requisicao negada --
    # negar nao deve consumir credito, senao um cliente insistente atrasaria os
    # outros indefinidamente.
    return BucketDecision(
        allowed=False,
        tokens_remaining=available,
        retry_after_ms=retry_after_ms,
        state=BucketState(tokens=available, updated_at_ms=now_ms),
    )
