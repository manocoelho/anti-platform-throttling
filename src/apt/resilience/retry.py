"""Politica de retry: backoff exponencial com jitter e escolha do degrau.

Duas responsabilidades:

1. `backoff_ms()` -- calcula quanto esperar antes da proxima tentativa.
2. `choose_tier()` -- traduz esse atraso no degrau de fila de retry correspondente
   (as filas tem TTL fixo; ver `messaging/topology.py`).

POR QUE JITTER, E NAO BACKOFF EXPONENCIAL PURO

Sem jitter, todos os clientes que falharam no mesmo instante voltam a tentar no
mesmo instante. E o "thundering herd": a plataforma que acabou de recusar 200
requisicoes recebe as mesmas 200 exatamente 1 segundo depois, todas juntas.
O backoff espaca as tentativas no tempo, mas mantem a sincronia entre elas --
que e justamente o que causa o problema.

O jitter quebra a sincronia. Usamos "full jitter":

    delay = random.uniform(0, min(cap, base * 2^tentativa))

O sorteio no intervalo INTEIRO (de 0 ao teto), e nao um ruido pequeno em torno
do teto, e o que dispersa de fato. E a variante recomendada pela AWS no artigo
classico "Exponential Backoff and Jitter", e a comparacao que ele apresenta
mostra por que: com jitter parcial os clientes ainda se agrupam perto do teto.

Trade-off: o full jitter pode sortear um atraso muito curto, quase zero. Para
uma tarefa isolada isso e ineficiente. Para o conjunto, e o que importa: a
media do intervalo cai pela metade, mas a variancia -- que e o que evita a
rajada sincronizada -- e maxima.

CASO ESPECIAL: `Retry-After`

Quando a plataforma responde 429 com o header `Retry-After`, ela esta nos
dizendo exatamente quanto esperar. Respeitamos esse valor em vez do nosso
calculo. Ignorar um `Retry-After` explicito e a forma mais rapida de escalar de
throttling para bloqueio -- a plataforma pediu, por escrito, um intervalo.
"""

from __future__ import annotations

import random

from apt.config import get_settings
from apt.messaging.topology import RETRY_TIERS_MS


def backoff_ms(attempt: int, *, base_ms: int | None = None, max_ms: int | None = None) -> int:
    """Atraso em milissegundos para a tentativa `attempt`, com full jitter.

    Args:
        attempt: numero da tentativa que ACABOU de falhar (1 = primeira).
        base_ms: atraso base. Padrao: `APT_RETRY_BASE_MS`.
        max_ms: teto do atraso. Padrao: `APT_RETRY_MAX_MS`.

    Returns:
        Milissegundos a esperar. Sempre >= 1 -- devolver 0 faria a proxima
        tentativa sair no mesmo instante, anulando o proposito do backoff.

    Exemplo com base=500ms:
        attempt=1 -> teto  500ms -> sorteio em [0,   500]
        attempt=2 -> teto 1000ms -> sorteio em [0,  1000]
        attempt=3 -> teto 2000ms -> sorteio em [0,  2000]
        attempt=4 -> teto 4000ms -> sorteio em [0,  4000]
    """
    settings = get_settings()
    base = base_ms if base_ms is not None else settings.retry_base_ms
    cap = max_ms if max_ms is not None else settings.retry_max_ms

    normalized = max(1, attempt)
    # `min` antes do sorteio, para o teto nunca explodir com tentativas altas.
    # 2**30 ja passa de qualquer cap razoavel; limitamos o expoente para evitar
    # um inteiro gigante inutil caso `attempt` venha corrompido de uma mensagem.
    ceiling = min(cap, base * (2 ** min(normalized - 1, 30)))
    return max(1, int(random.uniform(0, ceiling)))


def choose_tier(delay_ms: int) -> int:
    """Escolhe o degrau de fila de retry que melhor cobre `delay_ms`.

    As filas tem TTL fixo (1s, 5s, 30s), entao arredondamos para o primeiro
    degrau MAIOR OU IGUAL ao atraso desejado. Arredondar para cima, e nao para o
    mais proximo: esperar um pouco mais que o calculado e inofensivo, enquanto
    esperar menos significa voltar antes da plataforma ter se recuperado.

    Returns:
        Indice do degrau, de 1 a `len(RETRY_TIERS_MS)`.
    """
    for index, tier_ms in enumerate(RETRY_TIERS_MS, start=1):
        if delay_ms <= tier_ms:
            return index
    return len(RETRY_TIERS_MS)


def tier_for_attempt(attempt: int) -> tuple[int, int]:
    """Atalho: calcula o backoff da tentativa e o degrau correspondente.

    Returns:
        `(degrau, atraso_calculado_ms)`. O atraso calculado vai para o log --
        e o que permite depois explicar por que aquela tarefa caiu no degrau 3
        e nao no 2.
    """
    delay = backoff_ms(attempt)
    return choose_tier(delay), delay


def tier_for_retry_after(retry_after_ms: int) -> int:
    """Degrau que respeita um `Retry-After` informado pela plataforma.

    Sem jitter aqui, de proposito: quando a plataforma diz "espere 30s", o valor
    e uma instrucao, nao uma estimativa nossa. Sortear um numero menor que o
    pedido seria desobedecer.
    """
    return choose_tier(max(1, retry_after_ms))


def is_retryable_status(status_code: int) -> bool:
    """Decide se um status HTTP merece nova tentativa.

    Retentaveis:
        429 -- rate limit: e temporario por definicao.
        5xx -- erro do servidor: normalmente transitorio.
        408 -- request timeout.

    Nao retentaveis:
        4xx em geral -- 400 (payload invalido), 401/403 (credencial), 404 (URL
        inexistente). Nenhum deles muda de resultado ao ser repetido; retentar
        so gasta cota do rate limiter e atrasa as tarefas que poderiam ter
        sucesso. Vao direto para a DLQ, onde alguem pode olhar.
    """
    if status_code == 429 or status_code == 408:
        return True
    return 500 <= status_code < 600
