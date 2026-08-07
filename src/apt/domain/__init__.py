"""Dominio puro: enums, dataclasses e perfis de plataforma.

Nada aqui faz I/O. E o vocabulario compartilhado por API, worker e scheduler, e
pode ser importado num teste unitario sem Postgres, Redis ou RabbitMQ no ar.
"""

from apt.domain.models import (
    BreakerState,
    CampaignStatus,
    ControlMessage,
    ExecutionRecord,
    JitterStrategy,
    Outcome,
    Platform,
    SendTaskMessage,
    TaskStatus,
    utcnow,
)
from apt.domain.platforms import (
    PLATFORM_PROFILES,
    PlatformProfile,
    all_platforms,
    get_profile,
)

__all__ = [
    "PLATFORM_PROFILES",
    "BreakerState",
    "CampaignStatus",
    "ControlMessage",
    "ExecutionRecord",
    "JitterStrategy",
    "Outcome",
    "Platform",
    "PlatformProfile",
    "SendTaskMessage",
    "TaskStatus",
    "all_platforms",
    "get_profile",
    "utcnow",
]
