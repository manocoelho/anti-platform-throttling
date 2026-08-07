"""Endpoints de plataformas: thresholds e estado ao vivo dos mecanismos.

Este router junta, numa unica resposta, o que esta CONFIGURADO (thresholds no
Postgres) com o que esta ACONTECENDO (fichas no balde e estado do circuito, no
Redis). Ver os dois lados lado a lado e o que torna a demonstracao legivel: da
para mostrar `allowed_rps=16`, `available_tokens=3.2` e
`circuit_state=closed` e explicar o comportamento observado sem sair da tela.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, status

from apt.api.deps import BreakerDep, RateLimiterDep
from apt.api.schemas import PlatformOut, PlatformThresholdUpdate
from apt.db.engine import connection
from apt.db.repositories import PlatformRepository
from apt.domain.models import BreakerState, Platform
from apt.logging_setup import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/platforms", tags=["plataformas"])

PlatformParam = Annotated[Platform, Path(description="Plataforma alvo")]


def _safety_margin_percent(allowed: float, estimated: float | None) -> float:
    """Percentual do limite estimado que fica como folga.

    Devolve 0.0 quando nao ha limite estimado -- e nao um erro: `allowed_rps`
    continua sendo aplicado, apenas nao temos referencia para calcular a folga.
    """
    if not estimated or estimated <= 0:
        return 0.0
    return round(max(0.0, (1.0 - allowed / estimated)) * 100, 2)


async def _build_platform_out(
    row: dict[str, object], limiter: RateLimiterDep, breaker: BreakerDep
) -> PlatformOut:
    """Monta a resposta combinando configuracao (Postgres) e estado (Redis).

    As colunas numericas do Postgres chegam como `Decimal` (o tipo `NUMERIC`),
    entao passamos por `str` antes de converter -- `float(Decimal)` funciona, mas
    o tipo declarado da linha e `object` e converter via texto e o caminho que
    vale para qualquer representacao que o driver devolva.
    """
    platform = Platform(str(row["platform"]))
    allowed = float(str(row["allowed_rps"]))
    estimated = (
        float(str(row["estimated_limit_rps"]))
        if row.get("estimated_limit_rps") is not None
        else None
    )

    tokens = await limiter.peek(platform)
    snapshot = await breaker.snapshot(platform)
    raw_state = str(snapshot.get("state", "closed"))
    # O snapshot devolve "unknown" quando o Redis nao respondeu; nesse caso
    # omitimos o campo em vez de inventar um estado.
    circuit = BreakerState(raw_state) if raw_state in set(BreakerState) else None

    return PlatformOut(
        platform=platform,
        allowed_rps=allowed,
        burst_capacity=int(str(row["burst_capacity"])),
        estimated_limit_rps=estimated,
        safety_margin_percent=_safety_margin_percent(allowed, estimated),
        notes=str(row["notes"]) if row.get("notes") else None,
        available_tokens=round(tokens, 3) if tokens is not None else None,
        circuit_state=circuit,
    )


@router.get(
    "",
    response_model=list[PlatformOut],
    summary="Lista plataformas com threshold e estado ao vivo",
)
async def list_platforms(limiter: RateLimiterDep, breaker: BreakerDep) -> list[PlatformOut]:
    async with connection() as conn:
        rows = await PlatformRepository.list_all(conn)
    return [await _build_platform_out(row, limiter, breaker) for row in rows]


@router.get(
    "/{platform}",
    response_model=PlatformOut,
    summary="Detalha uma plataforma",
)
async def get_platform(
    platform: PlatformParam, limiter: RateLimiterDep, breaker: BreakerDep
) -> PlatformOut:
    async with connection() as conn:
        row = await PlatformRepository.get(conn, platform)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"plataforma '{platform}' nao cadastrada",
        )
    return await _build_platform_out(row, limiter, breaker)


@router.patch(
    "/{platform}",
    response_model=PlatformOut,
    summary="Ajusta o threshold de uma plataforma",
)
async def update_platform(
    platform: PlatformParam,
    payload: PlatformThresholdUpdate,
    limiter: RateLimiterDep,
    breaker: BreakerDep,
) -> PlatformOut:
    """Altera `allowed_rps` e/ou `burst_capacity` sem redeploy.

    ATENCAO A UM LIMITE CONHECIDO DESTA IMPLEMENTACAO

    A mudanca e gravada no banco, mas os workers leem os parametros do balde do
    proprio `.env` (`config.RateLimitConfig`) a cada chamada ao rate limiter.
    Portanto este endpoint altera o valor de REFERENCIA e a documentacao viva do
    sistema, mas nao muda o comportamento dos workers em execucao.

    Optamos por deixar essa limitacao explicita em vez de esconde-la. Fechar o
    ciclo exigiria os workers lerem o threshold do Redis (com o banco como fonte
    de verdade e um evento de fanout para invalidar cache) -- exatamente o
    mecanismo que ja existe nas feature flags. Ficou fora do escopo por tempo, e
    esta registrado em docs/TRADE-OFFS.md como evolucao natural.
    """
    if payload.allowed_rps is None and payload.burst_capacity is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="informe ao menos um campo: allowed_rps ou burst_capacity",
        )

    async with connection() as conn:
        updated = await PlatformRepository.update(
            conn,
            platform,
            allowed_rps=payload.allowed_rps,
            burst_capacity=payload.burst_capacity,
        )
    if updated is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"plataforma '{platform}' nao cadastrada",
        )

    logger.warning(
        "api.platform_threshold_updated",
        platform=str(platform),
        allowed_rps=payload.allowed_rps,
        burst_capacity=payload.burst_capacity,
        note="workers em execucao seguem usando os valores do .env",
    )
    return await _build_platform_out(updated, limiter, breaker)
