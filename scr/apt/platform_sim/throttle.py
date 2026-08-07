"""Motor do simulador: janela deslizante de contagem e injecao de falhas.

Este modulo representa o LADO DA PLATAFORMA. Ele e o adversario do nosso rate
limiter, e por isso implementa o algoritmo de forma deliberadamente DIFERENTE:

    nosso rate limiter  -> token bucket
    o simulador         -> janela deslizante (sliding window)

A assimetria e intencional e e o que torna o teste honesto. Se as duas pontas
usassem o mesmo algoritmo com os mesmos parametros, o nosso limiter acertaria o
limite por construcao -- o teste provaria apenas que 16 < 20, nao que o mecanismo
funciona.

Com algoritmos diferentes, as janelas de contagem nao se alinham. Uma rajada
permitida pelo nosso bucket (que tolera `capacity` requisicoes instantaneas) pode
cair inteira dentro da janela do simulador e estourar o limite dele. E
exatamente por isso que a margem de seguranca de 20% existe -- e o teste e o que
verifica se ela e suficiente.

MODOS DE INJECAO DE FALHA

    none          comportamento normal (so o limite de vazao)
    error_500     devolve 500 em toda requisicao -- simula plataforma fora do ar
    timeout       demora mais que o timeout do cliente -- simula degradacao
    throttle_hard devolve 429 em tudo -- simula penalizacao ja aplicada

Os modos existem para o teste de resiliencia: e assim que provocamos a abertura
do circuit breaker de forma controlada e reproduzivel.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum

from apt.logging_setup import get_logger

logger = get_logger(__name__)


class FaultMode(StrEnum):
    """Modos de falha injetavel."""

    NONE = "none"
    ERROR_500 = "error_500"
    TIMEOUT = "timeout"
    THROTTLE_HARD = "throttle_hard"


@dataclass(slots=True)
class FaultConfig:
    """Falha ativa numa plataforma.

    Attributes:
        mode: o modo de falha.
        expires_at: instante (monotonic) em que a falha se desativa sozinha.
            `None` = permanente ate ser removida.
    """

    mode: FaultMode = FaultMode.NONE
    expires_at: float | None = None

    @property
    def active(self) -> bool:
        """Se a falha esta valendo agora."""
        if self.mode is FaultMode.NONE:
            return False
        if self.expires_at is None:
            return True
        # A auto-expiracao existe para o teste de resiliencia: injetamos a falha
        # com TTL e observamos o circuito abrir e depois FECHAR sozinho, sem
        # precisar de uma segunda chamada no meio da medicao.
        return time.monotonic() < self.expires_at


@dataclass(slots=True)
class PlatformThrottle:
    """Contador de janela deslizante de uma plataforma.

    Guardamos o timestamp de cada requisicao aceita numa `deque` e, a cada nova
    requisicao, descartamos os que sairam da janela de 1 segundo. O tamanho da
    `deque` e a contagem atual.

    Este e justamente o algoritmo que NAO usamos no nosso rate limiter, e por um
    motivo que fica visivel aqui: a memoria cresce com o volume. A `deque` de uma
    plataforma a 20 req/s guarda 20 timestamps; a 20.000 req/s, guardaria 20.000.
    Para um simulador em ambiente controlado e irrelevante -- e a precisao exata
    da janela e desejavel para que a medicao seja confiavel. Ver ADR-004.

    Attributes:
        limit_rps: requisicoes aceitas por segundo antes de devolver 429.
        window: timestamps das requisicoes aceitas na janela corrente.
        total_accepted: acumulado de aceitas.
        total_throttled: acumulado de 429 devolvidos.
        peak_rps: maior contagem observada dentro de uma janela. E o numero mais
            interessante do relatorio: se o pico observado pela PLATAFORMA
            ficou abaixo do limite dela, o nosso rate limiter funcionou.
    """

    limit_rps: int
    window: deque[float] = field(default_factory=deque)
    total_accepted: int = 0
    total_throttled: int = 0
    peak_rps: int = 0

    def _evict_expired(self, now: float) -> None:
        """Remove da janela os timestamps com mais de 1 segundo."""
        cutoff = now - 1.0
        while self.window and self.window[0] < cutoff:
            self.window.popleft()

    def try_accept(self) -> tuple[bool, int]:
        """Tenta aceitar uma requisicao.

        Returns:
            `(aceita, retry_after_seconds)`. Quando recusada, o segundo valor e o
            que vai no header `Retry-After` -- calculado a partir do timestamp
            mais antigo da janela, que e quando abrira a primeira vaga.
        """
        now = time.monotonic()
        self._evict_expired(now)

        current = len(self.window)
        if current >= self.limit_rps:
            self.total_throttled += 1
            # Tempo restante para o timestamp mais antigo sair da janela.
            oldest = self.window[0] if self.window else now
            retry_after = max(1, int(1.0 - (now - oldest)) + 1)
            return False, retry_after

        self.window.append(now)
        self.total_accepted += 1
        self.peak_rps = max(self.peak_rps, len(self.window))
        return True, 0

    def current_rps(self) -> int:
        """Requisicoes na janela corrente, sem registrar nada."""
        self._evict_expired(time.monotonic())
        return len(self.window)

    def reset(self) -> None:
        """Zera contadores e janela. Usado entre cenarios de teste."""
        self.window.clear()
        self.total_accepted = 0
        self.total_throttled = 0
        self.peak_rps = 0