"""Endpoints administrativos: DLQ, eventos do breaker, metricas de teste e resets.

Este router e a janela de inspecao do sistema, e boa parte dele existe
especificamente para a apresentacao e para os testes de carga:

    /admin/failures         o que foi para a DLQ
    /admin/breaker-events   historico de transicoes do circuito
    /admin/latency          percentis p50/p95/p99 direto do Postgres
    /admin/throughput       envios por segundo -- a prova do rate limiter
    /admin/outcomes         contagem por resultado
    /admin/workers          distribuicao de carga entre replicas
    /admin/reset/*          zera estado entre cenarios de teste

NOTA DE SEGURANCA, HONESTA

Estes endpoints nao tem autenticacao. Num sistema real, `/admin/reset/*` atras de
uma rota publica seria uma falha grave -- qualquer um poderia zerar o rate
limiter e liberar uma rajada.

A POC roda em ambiente local, com as portas expostas apenas para a maquina de
desenvolvimento, e a autenticacao (OAuth2/JWT) ficou fora do escopo acordado
(ver docs/ATUALIZACOES-DOC-INICIAL.md). Registramos a lacuna aqui em vez de
finge-la resolvida: e uma limitacao conhecida, nao um descuido.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from apt.api.deps import BreakerDep, RateLimiterDep
from apt.api.schemas import (
    BreakerEventOut,
    FailureOut,
    LatencyStatsOut,
    ResetOut,
    ThroughputPointOut,
)
from apt.db.engine import connection
from apt.db.repositories import (
    BreakerEventRepository,
    ExecutionRepository,
    FailureRepository,
)
from apt.domain.models import Platform
from apt.logging_setup import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/admin", tags=["administracao"])


@router.get(
    "/failures",
    response_model=list[FailureOut],
    summary="Tarefas que esgotaram as tentativas (DLQ)",
)
async def list_failures(
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[FailureOut]:
    """Lista as falhas terminais nao resolvidas."""
    async with connection() as conn:
        rows = await FailureRepository.list_unresolved(conn, limit=limit)
    return [
        FailureOut(
            id=int(r["id"]),
            task_id=r["task_id"],
            campaign_id=r["campaign_id"],
            platform=Platform(str(r["platform"])),
            total_attempts=int(r["total_attempts"]),
            last_outcome=str(r["last_outcome"]),
            last_error=r["last_error"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


@router.get(
    "/breaker-events",
    response_model=list[BreakerEventOut],
    summary="Historico de transicoes do circuit breaker",
)
async def list_breaker_events(
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> list[BreakerEventOut]:
    """Transicoes do circuito, das mais recentes para as mais antigas.

    E a evidencia usada no teste de resiliencia: depois de injetar falha, esta
    lista deve mostrar `closed -> open` e, apos a recuperacao,
    `open -> half_open -> closed`.
    """
    async with connection() as conn:
        rows = await BreakerEventRepository.recent(conn, limit=limit)
    return [BreakerEventOut(**row) for row in rows]


@router.get(
    "/latency",
    response_model=LatencyStatsOut,
    summary="Percentis de latencia dos envios aceitos",
)
async def get_latency(
    platform: Annotated[Platform | None, Query(description="Filtra por plataforma")] = None,
) -> LatencyStatsOut:
    """p50/p95/p99 calculados pelo Postgres com `percentile_cont`.

    Considera apenas `outcome = 'sent'` -- incluir timeouts distorceria o p99,
    porque um timeout registra o teto que nos configuramos, nao a latencia do
    servico. Ver `ExecutionRepository.latency_percentiles`.
    """
    async with connection() as conn:
        stats = await ExecutionRepository.latency_percentiles(conn, platform=platform)
    return LatencyStatsOut(platform=platform, **stats)  # type: ignore[arg-type]


@router.get(
    "/throughput",
    response_model=list[ThroughputPointOut],
    summary="Envios aceitos por segundo (a prova do rate limiter)",
)
async def get_throughput(
    platform: Annotated[Platform, Query(description="Plataforma")],
    window_seconds: Annotated[int, Query(ge=5, le=3600)] = 60,
) -> list[ThroughputPointOut]:
    """Serie de envios aceitos por segundo na janela recente.

    E a consulta central do teste de escala: o valor MAXIMO desta serie deve
    ficar abaixo do `allowed_rps` da plataforma, rodando com 1 worker ou com 5.
    """
    async with connection() as conn:
        points = await ExecutionRepository.throughput_per_second(
            conn, platform=platform, window_seconds=window_seconds
        )
    return [ThroughputPointOut(**p) for p in points]


@router.get(
    "/outcomes",
    response_model=dict[str, int],
    summary="Contagem de tentativas por resultado",
)
async def get_outcomes(
    platform: Annotated[Platform | None, Query()] = None,
) -> dict[str, int]:
    """Contagem por resultado.

    A leitura que interessa: `throttled` proximo de zero (nao fomos bloqueados) e
    `rate_limited_local` positivo (nos autolimitamos). Os dois numeros juntos
    contam a historia da POC.
    """
    async with connection() as conn:
        return await ExecutionRepository.outcome_breakdown(conn, platform=platform)


@router.get(
    "/workers",
    response_model=dict[str, int],
    summary="Distribuicao de envios entre as replicas de worker",
)
async def get_worker_distribution() -> dict[str, int]:
    """Quantos envios cada replica atendeu.

    E a evidencia do padrao Load Balancing: com `prefetch=1`, a distribuicao
    entre as replicas deve ficar aproximadamente uniforme.
    """
    async with connection() as conn:
        return await ExecutionRepository.worker_distribution(conn)


@router.post(
    "/reset/rate-limiter",
    response_model=ResetOut,
    summary="Zera os token buckets",
)
async def reset_rate_limiter(
    limiter: RateLimiterDep,
    platform: Annotated[Platform | None, Query()] = None,
) -> ResetOut:
    """Apaga os baldes, fazendo-os renascer cheios.

    Necessario entre cenarios de teste: sem isso, o balde parcialmente drenado
    pelo cenario anterior contaminaria a medicao do seguinte.
    """
    removed = await limiter.reset(platform)
    logger.warning("api.rate_limiter_reset", platform=str(platform or "all"), removed=removed)
    return ResetOut(
        removed=removed,
        message=f"{removed} balde(s) removido(s); renascerao cheios no proximo uso.",
    )


@router.post(
    "/reset/circuit-breaker",
    response_model=ResetOut,
    summary="Forca o fechamento dos circuitos",
)
async def reset_circuit_breaker(
    breaker: BreakerDep,
    platform: Annotated[Platform | None, Query()] = None,
) -> ResetOut:
    """Apaga o estado do circuito, fechando-o imediatamente.

    Dois usos: limpar o estado entre cenarios de teste e, operacionalmente,
    permitir que alguem que SABE que a plataforma voltou nao precise esperar o
    cooldown terminar.
    """
    removed = await breaker.reset(platform)
    logger.warning("api.circuit_breaker_reset", platform=str(platform or "all"), removed=removed)
    return ResetOut(
        removed=removed,
        message=f"{removed} circuito(s) resetado(s) para o estado closed.",
    )
