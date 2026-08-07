"""Endpoints de saude: `/health/live` e `/health/ready`.

A distincao entre os dois nao e formalidade -- ela determina a acao correta
quando algo falha:

    /health/live   "o processo esta vivo?"
                   NAO checa dependencias. Responde 200 enquanto o event loop
                   estiver respondendo.
                   Falha -> REINICIE o container.

    /health/ready  "posso receber trafego?"
                   Checa Postgres, Redis e o dispatcher.
                   Falha -> PARE de mandar trafego, mas NAO reinicie.

Por que a separacao importa: se `live` checasse o banco, uma indisponibilidade
de 30 segundos do Postgres faria o orquestrador reiniciar todos os containers da
aplicacao -- que voltariam e falhariam de novo, porque o problema nunca esteve
neles. E o classico loop de restart causado por health check mal desenhado.

O `docker-compose.yml` usa `live` no healthcheck exatamente por isso.
"""

from __future__ import annotations

from fastapi import APIRouter, Response, status

from apt import __version__
from apt.api.deps import StateDep
from apt.api.schemas import HealthOut
from apt.config import get_settings
from apt.db.engine import check_health as db_health
from apt.observability.metrics import CONTENT_TYPE, render_metrics
from apt.resilience.redis_client import check_health as redis_health

router = APIRouter(tags=["saude"])


@router.get(
    "/health/live",
    response_model=HealthOut,
    summary="Liveness: o processo esta vivo?",
)
async def liveness() -> HealthOut:
    """Responde 200 sem tocar em nenhuma dependencia.

    Deliberadamente trivial. Qualquer verificacao aqui -- por mais leve que
    pareca -- transformaria falha de dependencia em reinicio de container.
    """
    settings = get_settings()
    return HealthOut(status="alive", service=settings.service_name, version=__version__)


@router.get(
    "/health/ready",
    response_model=HealthOut,
    summary="Readiness: pode receber trafego?",
)
async def readiness(state: StateDep, response: Response) -> HealthOut:
    """Verifica Postgres, Redis e o dispatcher.

    Devolve 503 quando alguma verificacao falha. O corpo continua sendo um
    `HealthOut` completo -- com 503 e o detalhe de QUAL check falhou -- porque um
    health check que devolve apenas o status obriga quem investiga a ir ao log
    para descobrir a causa.
    """
    settings = get_settings()

    db_ok = await db_health()
    redis_ok = await redis_health()
    # O dispatcher pode estar ausente se o startup falhou parcialmente.
    dispatcher_ok = state.dispatcher is not None

    checks = {
        "postgres": db_ok,
        "redis": redis_ok,
        "dispatcher": dispatcher_ok,
    }
    all_ok = all(checks.values())

    if not all_ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthOut(
        status="ready" if all_ok else "not_ready",
        service=settings.service_name,
        version=__version__,
        checks=checks,
        details={
            "dispatcher_ticks": state.dispatcher.ticks if state.dispatcher else None,
            "startup_errors": len(state.startup_errors),
        },
    )


@router.get(
    "/metrics",
    summary="Metricas no formato Prometheus",
    # Excluido do OpenAPI: a saida nao e JSON e poluiria o `/docs` com um schema
    # que nao descreve nada util.
    include_in_schema=False,
)
async def metrics_endpoint() -> Response:
    """Expoe as metricas para o Prometheus raspar."""
    return Response(content=render_metrics(), media_type=CONTENT_TYPE)
