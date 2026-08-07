"""Circuit breaker distribuido, com um circuito independente por plataforma.

A separacao por plataforma e o que junta este padrao ao Bulkhead: existe um
circuito para o YouTube e outro para o Instagram. Quando o Instagram degrada,
apenas o circuito dele abre -- o YouTube continua enviando na vazao normal. Um
circuito unico compartilhado transformaria a falha de uma plataforma em parada
total do sistema, que e exatamente o efeito em cascata que o padrao existe para
impedir.

A logica das transicoes esta documentada em `breaker_state.py` (implementacao de
referencia, pura e testavel) e executada atomicamente em `lua/circuit_breaker.lua`.
Este modulo e a fachada: traduz chamadas Python em invocacoes do script, emite
metricas e persiste as transicoes em `breaker_events` para a apresentacao.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from apt.config import get_settings
from apt.domain.models import BreakerState, Platform
from apt.logging_setup import get_logger
from apt.resilience.redis_client import get_redis, load_script

logger = get_logger(__name__)

_SCRIPT_NAME = "circuit_breaker.lua"

# TTL do estado do circuito. Precisa ser confortavelmente maior que
# `open_seconds`, senao um circuito aberto expiraria durante o proprio cooldown
# e voltaria a "closed" sem ter sondado nada.
_STATE_TTL_SECONDS = 600


@dataclass(frozen=True, slots=True)
class BreakerDecision:
    """Resultado de uma consulta ao circuito.

    Attributes:
        allowed: se o envio pode seguir.
        state: estado do circuito apos a consulta.
        retry_after_ms: quando negado, quanto falta para a proxima sondagem.
        failure_count: falhas consecutivas registradas.
        transition: `(de, para)` se esta chamada mudou o estado, senao `None`.
    """

    allowed: bool
    state: BreakerState
    retry_after_ms: int = 0
    failure_count: int = 0
    transition: tuple[BreakerState, BreakerState] | None = None


def breaker_key(platform: Platform) -> str:
    """Chave Redis do circuito de uma plataforma."""
    return f"apt:cb:{platform}"


class CircuitBreaker:
    """Fachada do circuit breaker distribuido."""

    def __init__(self, *, observer_id: str = "unknown") -> None:
        """
        Args:
            observer_id: identificador do processo que observa (ex.:
                "worker-a1b2"). Vai para `breaker_events.observed_by`, e e o que
                permite responder na apresentacao "qual replica viu a falha que
                abriu o circuito?".
        """
        self._settings = get_settings()
        self._observer_id = observer_id
        # Cache do ultimo estado conhecido por plataforma. Serve apenas para
        # metricas e para o modo degradado -- NUNCA para decidir. Decidir a
        # partir de cache local reintroduziria o problema do breaker por
        # processo que este modulo existe para resolver.
        self._last_known: dict[Platform, BreakerState] = {}

    async def _call(self, platform: Platform, op: str) -> BreakerDecision:
        """Invoca o script Lua e traduz o retorno."""
        cfg = self._settings.circuit_breaker
        script = load_script(_SCRIPT_NAME)
        now_ms = int(time.time() * 1000)

        raw = await script(
            keys=[breaker_key(platform)],
            args=[
                op,
                now_ms,
                cfg.failure_threshold,
                cfg.open_seconds,
                cfg.half_open_probes,
                cfg.success_threshold,
                _STATE_TTL_SECONDS,
            ],
        )
        allowed_flag, state_raw, retry_after_ms, failures, transitioned, from_raw = raw

        state = BreakerState(str(state_raw))
        self._last_known[platform] = state

        transition: tuple[BreakerState, BreakerState] | None = None
        if int(transitioned) == 1:
            transition = (BreakerState(str(from_raw)), state)

        return BreakerDecision(
            allowed=bool(int(allowed_flag)),
            state=state,
            retry_after_ms=int(retry_after_ms),
            failure_count=int(failures),
            transition=transition,
        )

    async def allow(self, platform: Platform) -> BreakerDecision:
        """Pergunta se um envio pode passar pelo circuito.

        Em caso de falha do Redis, PERMITE (fail-open) -- mesma politica do rate
        limiter, pela mesma razao: negar tudo transformaria uma queda de Redis
        em indisponibilidade total. O detalhe importante e que o fail-open aqui
        e menos arriscado do que parece: se a plataforma realmente estiver com
        problema, os 429/5xx continuam chegando, o retry com backoff continua
        espacando as tentativas e o bulkhead continua limitando a concorrencia.
        Perdemos a antecipacao, nao todas as defesas.
        """
        try:
            decision = await self._call(platform, "allow")
        except Exception as exc:
            logger.error(
                "circuit_breaker.unavailable_fail_open",
                platform=str(platform),
                error=str(exc),
            )
            return BreakerDecision(allowed=True, state=BreakerState.CLOSED)

        if decision.transition is not None:
            self._log_transition(platform, decision, reason="cooldown cumprido")
        return decision

    async def record_success(self, platform: Platform) -> BreakerDecision:
        """Registra um envio bem-sucedido."""
        try:
            decision = await self._call(platform, "success")
        except Exception as exc:
            logger.warning(
                "circuit_breaker.record_success_failed",
                platform=str(platform),
                error=str(exc),
            )
            return BreakerDecision(allowed=True, state=BreakerState.CLOSED)

        if decision.transition is not None:
            self._log_transition(platform, decision, reason="sondas de recuperacao confirmadas")
        return decision

    async def record_failure(
        self, platform: Platform, *, reason: str = "platform_rejection"
    ) -> BreakerDecision:
        """Registra uma rejeicao da plataforma (429, 5xx ou timeout).

        Somente resultados de `Outcome.is_platform_rejection` devem chegar aqui.
        Passar um adiamento interno (rate limiter, bulkhead) faria o sistema
        abrir o proprio circuito ao se autolimitar -- ver o docstring de
        `breaker_state.py`.
        """
        try:
            decision = await self._call(platform, "failure")
        except Exception as exc:
            logger.warning(
                "circuit_breaker.record_failure_failed",
                platform=str(platform),
                error=str(exc),
            )
            return BreakerDecision(allowed=True, state=BreakerState.CLOSED)

        if decision.transition is not None:
            self._log_transition(platform, decision, reason=reason)
        return decision

    async def snapshot(self, platform: Platform) -> dict[str, object]:
        """Le o estado do circuito sem alterar nada. Usado pela API e metricas."""
        try:
            raw = await get_redis().hgetall(breaker_key(platform))
        except Exception as exc:
            logger.warning("circuit_breaker.snapshot_failed", error=str(exc))
            return {"platform": str(platform), "state": "unknown"}

        if not raw:
            # Chave ausente = nunca houve falha (ou o TTL expirou). Fechado.
            return {
                "platform": str(platform),
                "state": str(BreakerState.CLOSED),
                "failure_count": 0,
                "success_count": 0,
                "probes_in_flight": 0,
                "opened_at_ms": None,
            }

        return {
            "platform": str(platform),
            "state": raw.get("state", str(BreakerState.CLOSED)),
            "failure_count": int(raw.get("failures", 0) or 0),
            "success_count": int(raw.get("successes", 0) or 0),
            "probes_in_flight": int(raw.get("probes", 0) or 0),
            "opened_at_ms": int(raw.get("opened_at", 0) or 0) or None,
        }

    async def reset(self, platform: Platform | None = None) -> int:
        """Forca o circuito a fechar apagando o estado.

        Usado pelos testes de resiliencia (para comecar cada cenario com o
        circuito limpo) e exposto na API como acao administrativa, para o caso
        de um operador saber que a plataforma voltou antes do cooldown terminar.
        """
        client = get_redis()
        if platform is not None:
            removed = int(await client.delete(breaker_key(platform)))
        else:
            removed = 0
            async for key in client.scan_iter(match="apt:cb:*", count=100):
                removed += int(await client.delete(key))
        logger.info("circuit_breaker.reset", platform=str(platform or "all"), removed=removed)
        return removed

    def last_known_state(self, platform: Platform) -> BreakerState | None:
        """Ultimo estado observado por ESTE processo (so para metricas)."""
        return self._last_known.get(platform)

    def _log_transition(
        self, platform: Platform, decision: BreakerDecision, *, reason: str
    ) -> None:
        """Loga a transicao em nivel WARNING.

        WARNING e nao INFO de proposito: uma transicao de circuito e sempre
        digna de atencao operacional, tanto abrir (a plataforma esta com
        problema) quanto fechar (acabamos de sair de um periodo degradado).
        """
        assert decision.transition is not None
        from_state, to_state = decision.transition
        logger.warning(
            "circuit_breaker.transition",
            platform=str(platform),
            from_state=str(from_state),
            to_state=str(to_state),
            failure_count=decision.failure_count,
            reason=reason,
            observed_by=self._observer_id,
        )


_breaker: CircuitBreaker | None = None


def get_circuit_breaker(*, observer_id: str = "unknown") -> CircuitBreaker:
    """Devolve o circuit breaker do processo.

    O `observer_id` so tem efeito na primeira chamada, quando a instancia e
    criada -- e o comportamento desejado: cada processo tem um identificador
    fixo, definido no boot.
    """
    global _breaker
    if _breaker is None:
        _breaker = CircuitBreaker(observer_id=observer_id)
    return _breaker
