"""Simulador das plataformas externas (YouTube e Instagram).

POR QUE UM SIMULADOR PROPRIO, E NAO MOCKS

Um mock em teste unitario nao produz o fenomeno que a POC estuda. Throttling e
um comportamento EMERGENTE: depende de quantas requisicoes chegam, com que
espacamento, dentro de qual janela. Um mock que devolve 429 quando mandamos dizer
prova apenas que o nosso codigo trata 429 -- nao que o nosso rate limiter evita
receber 429.

E por que nao usar as plataformas de verdade: enviar volume de trafego artificial
a APIs de terceiros para descobrir os limites delas seria, no minimo, uso
abusivo de servico alheio -- e provavelmente violacao de termos de uso. Nao e uma
opcao num trabalho academico. Ver ADR-008.

O simulador implementa janela deslizante (o nosso limiter usa token bucket) -- a
assimetria e deliberada, e o motivo esta no docstring de `throttle.py`.

ENDPOINTS

    POST /youtube/engagements     recebe um envio
    POST /instagram/engagements   recebe um envio
    POST /admin/fault             injeta falha (para o teste de resiliencia)
    DELETE /admin/fault/{plat}    remove a falha
    POST /admin/reset             zera contadores
    GET  /admin/stats             estatisticas -- inclui o `peak_rps` observado
    GET  /health                  saude
    GET  /metrics                 metricas Prometheus
"""

from __future__ import annotations

import asyncio
import random
from typing import Annotated

from fastapi import Body, FastAPI, Path, Response, status
from pydantic import BaseModel, Field

from apt import __version__
from apt.config import get_settings
from apt.domain.models import Platform
from apt.domain.platforms import PLATFORM_PROFILES, all_platforms
from apt.logging_setup import configure_logging, get_logger
from apt.observability.metrics import CONTENT_TYPE, render_metrics, sim_active_faults, sim_requests
from apt.platform_sim.throttle import FaultConfig, FaultMode, PlatformThrottle

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Estado do simulador (em memoria, por processo)
# ---------------------------------------------------------------------------
class SimulatorState:
    """Contadores e falhas por plataforma.

    Estado em memoria de proposito: o simulador e um dublê de teste, e persistir
    o contador dele sobreviveria entre cenarios e contaminaria a medicao. Um
    restart deve zerar tudo.
    """

    def __init__(self) -> None:
        settings = get_settings()
        # Os limites vem dos perfis de dominio, nao de configuracao propria --
        # assim `estimated_limit_rps` tem UM lugar de definicao e nao ha risco de
        # o simulador aplicar um limite diferente do documentado.
        self.throttles: dict[Platform, PlatformThrottle] = {
            platform: PlatformThrottle(limit_rps=int(profile.estimated_limit_rps))
            for platform, profile in PLATFORM_PROFILES.items()
        }
        self.faults: dict[Platform, FaultConfig] = {
            platform: FaultConfig() for platform in all_platforms()
        }
        self.latency_min_ms = settings.sim_latency_min_ms
        self.latency_max_ms = settings.sim_latency_max_ms
        self._rng = random.Random()

    async def simulate_latency(self) -> None:
        """Introduz latencia artificial.

        Sem isso, o simulador responderia em microssegundos e os percentis de
        latencia do relatorio seriam todos zero -- inuteis para demonstrar que a
        medicao funciona.
        """
        delay_ms = self._rng.uniform(self.latency_min_ms, self.latency_max_ms)
        await asyncio.sleep(delay_ms / 1000.0)


state = SimulatorState()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class EngagementIn(BaseModel):
    """Corpo de um envio."""

    content_url: str
    task_id: str
    correlation_id: str = ""


class EngagementOut(BaseModel):
    """Resposta de um envio aceito."""

    accepted: bool
    platform: Platform
    task_id: str
    current_rps: int
    limit_rps: int


class FaultIn(BaseModel):
    """Corpo de `POST /admin/fault`."""

    platform: Platform
    mode: FaultMode
    ttl_seconds: int | None = Field(
        default=None,
        ge=1,
        le=3600,
        description=(
            "Se informado, a falha se desativa sozinha apos este tempo. E o que "
            "permite ao teste de resiliencia observar o circuito abrir E fechar "
            "numa unica execucao, sem intervencao no meio da medicao."
        ),
    )


class FaultOut(BaseModel):
    """Estado de falha de uma plataforma."""

    platform: Platform
    mode: FaultMode
    active: bool
    ttl_seconds: int | None = None


class PlatformStats(BaseModel):
    """Estatisticas de uma plataforma."""

    platform: Platform
    limit_rps: int
    current_rps: int
    peak_rps: int = Field(
        description=(
            "Maior contagem observada numa janela de 1s. Se este valor ficou "
            "ABAIXO de limit_rps durante o teste, o rate limiter do cliente "
            "cumpriu o objetivo."
        )
    )
    total_accepted: int
    total_throttled: int
    fault_mode: FaultMode


# ---------------------------------------------------------------------------
# Aplicacao
# ---------------------------------------------------------------------------
def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(
        service_name=settings.service_name,
        level=settings.log_level,
        as_json=settings.log_json,
    )

    app = FastAPI(
        title="Simulador de Plataformas Externas",
        version=__version__,
        description=(
            "Dublê das plataformas externas para a POC 4. Aplica limite por "
            "JANELA DESLIZANTE (o cliente usa token bucket -- a assimetria e "
            "deliberada) e permite injecao de falhas.\n\n"
            "Os limites sao ESTIMATIVAS para ambiente controlado, nao numeros "
            "oficiais de nenhuma plataforma real."
        ),
    )

    # -----------------------------------------------------------------------
    # Recebimento de envios
    # -----------------------------------------------------------------------
    async def _handle_engagement(
        platform: Platform, payload: EngagementIn, response: Response
    ) -> EngagementOut | dict[str, object]:
        """Logica compartilhada pelos endpoints das duas plataformas."""
        throttle = state.throttles[platform]
        fault = state.faults[platform]

        # --- Falha injetada tem precedencia sobre tudo ---------------------
        if fault.active:
            sim_active_faults.labels(platform=str(platform)).set(1)

            if fault.mode is FaultMode.TIMEOUT:
                # Dorme mais que o timeout do cliente (5s). O cliente aborta
                # antes e registra Outcome.TIMEOUT.
                await asyncio.sleep(settings.send_timeout_seconds + 2.0)
                sim_requests.labels(platform=str(platform), status="timeout").inc()
                response.status_code = status.HTTP_504_GATEWAY_TIMEOUT
                return {"error": "gateway timeout (falha injetada)"}

            if fault.mode is FaultMode.ERROR_500:
                sim_requests.labels(platform=str(platform), status="500").inc()
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                return {"error": "erro interno da plataforma (falha injetada)"}

            if fault.mode is FaultMode.THROTTLE_HARD:
                throttle.total_throttled += 1
                sim_requests.labels(platform=str(platform), status="429").inc()
                response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
                response.headers["Retry-After"] = "5"
                return {"error": "rate limit excedido (falha injetada)"}
        else:
            sim_active_faults.labels(platform=str(platform)).set(0)

        # --- Comportamento normal: janela deslizante -----------------------
        accepted, retry_after = throttle.try_accept()

        if not accepted:
            sim_requests.labels(platform=str(platform), status="429").inc()
            response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
            # `Retry-After` explicito: e informacao que uma plataforma real
            # fornece, e o nosso worker tem codigo para respeita-la.
            response.headers["Retry-After"] = str(retry_after)
            logger.warning(
                "sim.throttled",
                platform=str(platform),
                task_id=payload.task_id,
                limit_rps=throttle.limit_rps,
                retry_after=retry_after,
                note="o cliente excedeu o limite desta plataforma",
            )
            return {
                "error": "rate limit excedido",
                "limit_rps": throttle.limit_rps,
                "retry_after": retry_after,
            }

        await state.simulate_latency()
        sim_requests.labels(platform=str(platform), status="200").inc()

        return EngagementOut(
            accepted=True,
            platform=platform,
            task_id=payload.task_id,
            current_rps=throttle.current_rps(),
            limit_rps=throttle.limit_rps,
        )

    @app.post("/youtube/engagements", tags=["plataformas"], summary="Envio para o YouTube")
    async def youtube_engagement(
        payload: EngagementIn, response: Response
    ) -> EngagementOut | dict[str, object]:
        return await _handle_engagement(Platform.YOUTUBE, payload, response)

    @app.post("/instagram/engagements", tags=["plataformas"], summary="Envio para o Instagram")
    async def instagram_engagement(
        payload: EngagementIn, response: Response
    ) -> EngagementOut | dict[str, object]:
        return await _handle_engagement(Platform.INSTAGRAM, payload, response)

    # -----------------------------------------------------------------------
    # Administracao
    # -----------------------------------------------------------------------
    @app.post(
        "/admin/fault",
        response_model=FaultOut,
        tags=["administracao"],
        summary="Injeta uma falha numa plataforma",
    )
    async def inject_fault(payload: Annotated[FaultIn, Body()]) -> FaultOut:
        """Ativa um modo de falha, opcionalmente com auto-expiracao."""
        import time

        expires_at = time.monotonic() + payload.ttl_seconds if payload.ttl_seconds else None
        state.faults[payload.platform] = FaultConfig(mode=payload.mode, expires_at=expires_at)
        sim_active_faults.labels(platform=str(payload.platform)).set(
            0 if payload.mode is FaultMode.NONE else 1
        )
        logger.warning(
            "sim.fault_injected",
            platform=str(payload.platform),
            mode=str(payload.mode),
            ttl_seconds=payload.ttl_seconds,
        )
        return FaultOut(
            platform=payload.platform,
            mode=payload.mode,
            active=state.faults[payload.platform].active,
            ttl_seconds=payload.ttl_seconds,
        )

    @app.delete(
        "/admin/fault/{platform}",
        response_model=FaultOut,
        tags=["administracao"],
        summary="Remove a falha de uma plataforma",
    )
    async def clear_fault(
        platform: Annotated[Platform, Path(description="Plataforma")],
    ) -> FaultOut:
        state.faults[platform] = FaultConfig()
        sim_active_faults.labels(platform=str(platform)).set(0)
        logger.info("sim.fault_cleared", platform=str(platform))
        return FaultOut(platform=platform, mode=FaultMode.NONE, active=False)

    @app.post(
        "/admin/reset",
        tags=["administracao"],
        summary="Zera contadores e falhas de todas as plataformas",
    )
    async def reset_all() -> dict[str, str]:
        """Volta o simulador ao estado inicial.

        Chamado no inicio de cada cenario de teste. Sem isso, o `peak_rps` de um
        teste anterior apareceria no relatorio do seguinte.
        """
        for throttle in state.throttles.values():
            throttle.reset()
        for platform in all_platforms():
            state.faults[platform] = FaultConfig()
            sim_active_faults.labels(platform=str(platform)).set(0)
        logger.info("sim.reset")
        return {"status": "reset", "message": "contadores e falhas zerados"}

    @app.get(
        "/admin/stats",
        response_model=list[PlatformStats],
        tags=["administracao"],
        summary="Estatisticas por plataforma",
    )
    async def get_stats() -> list[PlatformStats]:
        """Estatisticas do ponto de vista da PLATAFORMA.

        `peak_rps` e o numero central do relatorio: e o pico que a plataforma
        realmente observou. Se ficou abaixo de `limit_rps` durante todo o teste,
        o rate limiter do cliente cumpriu o objetivo -- e essa e a evidencia
        medida do lado de quem imporia a punicao.
        """
        return [
            PlatformStats(
                platform=platform,
                limit_rps=throttle.limit_rps,
                current_rps=throttle.current_rps(),
                peak_rps=throttle.peak_rps,
                total_accepted=throttle.total_accepted,
                total_throttled=throttle.total_throttled,
                fault_mode=(
                    state.faults[platform].mode if state.faults[platform].active else FaultMode.NONE
                ),
            )
            for platform, throttle in state.throttles.items()
        ]

    # -----------------------------------------------------------------------
    # Saude e metricas
    # -----------------------------------------------------------------------
    @app.get("/health", tags=["saude"], summary="Saude do simulador")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "platform-sim", "version": __version__}

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint() -> Response:
        return Response(content=render_metrics(), media_type=CONTENT_TYPE)

    return app


app = create_app()