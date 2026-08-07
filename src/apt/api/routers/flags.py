"""Endpoints de feature flags.

O fluxo completo de uma alteracao, que e o que torna o padrao util aqui:

    PATCH /flags/jitter_enabled {"value": false}
      1. grava no Redis          (FeatureFlags.set)
      2. invalida o cache local da API
      3. publica `flags_changed` no exchange FANOUT
      4. todos os workers recebem e invalidam o cache deles
      5. o proximo envio de qualquer worker ja usa o valor novo

O passo 3 e o que faz a propagacao ser imediata em vez de depender do TTL de 2
segundos do cache. E precisa ser fanout: com um exchange topic e fila
compartilhada, apenas UM worker receberia o aviso. Ver
`resilience/feature_flags.py`.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, status

from apt.api.deps import FlagsDep, PublisherDep
from apt.api.schemas import FlagsOut, FlagUpdate
from apt.domain.models import ControlMessage
from apt.logging_setup import get_logger
from apt.resilience.feature_flags import DEFAULT_FLAGS

logger = get_logger(__name__)

router = APIRouter(prefix="/flags", tags=["feature flags"])

FlagName = Annotated[
    str,
    Path(
        description=(
            "Nome da flag. Validas: rate_limiter_enabled, "
            "circuit_breaker_enabled, jitter_enabled, auto_pause_on_open, "
            "dispatch_enabled"
        )
    ),
]


@router.get("", response_model=FlagsOut, summary="Lista as feature flags")
async def list_flags(flags: FlagsDep) -> FlagsOut:
    """Le todas as flags direto do Redis (ignorando o cache)."""
    return FlagsOut(flags=await flags.all_flags())


@router.patch(
    "/{flag}",
    response_model=FlagsOut,
    summary="Altera uma feature flag em runtime",
)
async def update_flag(
    flag: FlagName,
    payload: FlagUpdate,
    flags: FlagsDep,
    publisher: PublisherDep,
) -> FlagsOut:
    """Altera a flag e propaga a mudanca a todos os workers.

    Desligar `rate_limiter_enabled` ou `circuit_breaker_enabled` remove uma
    protecao do sistema, por isso o log sai em nivel WARNING -- uma protecao
    desligada por engano e deixada assim e o tipo de coisa que ninguem deveria
    descobrir tarde.
    """
    try:
        updated = await flags.set(flag, payload.value)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"flag desconhecida '{flag}'. Validas: {sorted(DEFAULT_FLAGS)}",
        ) from exc

    # Difunde a invalidacao. Sem este passo, os workers levariam ate 2s (o TTL do
    # cache) para enxergar o valor novo -- o que na demonstracao ao vivo apareceria
    # como "nao mudou nada".
    await publisher.publish_control(
        ControlMessage(type="flags_changed", payload={"flag": flag, "value": payload.value})
    )

    log = logger.warning if not payload.value else logger.info
    log(
        "api.flag_updated",
        flag=flag,
        value=payload.value,
        note="protecao desligada" if not payload.value else "protecao ativa",
    )
    return FlagsOut(flags=updated)


@router.post(
    "/reset",
    response_model=FlagsOut,
    summary="Restaura todas as flags para o padrao",
)
async def reset_flags(flags: FlagsDep, publisher: PublisherDep) -> FlagsOut:
    """Apaga as flags do Redis, voltando todas ao padrao (protecoes ligadas).

    Usado entre cenarios de teste, para garantir que um teste nao herda uma
    protecao desligada pelo anterior.
    """
    restored = await flags.reset()
    await publisher.publish_control(ControlMessage(type="flags_changed", payload={}))
    logger.info("api.flags_reset", flags=restored)
    return FlagsOut(flags=restored)
