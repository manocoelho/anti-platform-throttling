"""Camada de persistencia (PostgreSQL via SQLAlchemy assincrono).

Todo o SQL do sistema vive em `repositories.py`. Nenhum modulo fora deste
pacote monta consulta -- ver o docstring de `repositories` para o motivo.
"""

from apt.db.engine import (
    check_health,
    connection,
    dispose_engine,
    get_engine,
    get_session_factory,
)
from apt.db.repositories import (
    BreakerEventRepository,
    CampaignRepository,
    ContentRepository,
    ExecutionRepository,
    FailureRepository,
    PlatformRepository,
    TaskRepository,
)

__all__ = [
    "BreakerEventRepository",
    "CampaignRepository",
    "ContentRepository",
    "ExecutionRepository",
    "FailureRepository",
    "PlatformRepository",
    "TaskRepository",
    "check_health",
    "connection",
    "dispose_engine",
    "get_engine",
    "get_session_factory",
]
