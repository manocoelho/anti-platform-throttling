"""Distribuicao temporal e materializacao de campanhas.

`jitter.py` responde "quando cada envio deve sair?" -- e onde vive o item
"distribuicao temporal com jitter variavel" do escopo da POC 4.

`dispatcher.py` transforma campanhas em tarefas individuais e as publica na
fila. Roda como background task da API, nao como servico separado (ADR-010).
"""

from apt.scheduling.dispatcher import Dispatcher
from apt.scheduling.jitter import (
    HOURLY_ACTIVITY_PROFILE,
    JitterPlan,
    activity_multiplier,
    exponential_offsets,
    plan_tick,
    uniform_offsets,
)

__all__ = [
    "HOURLY_ACTIVITY_PROFILE",
    "Dispatcher",
    "JitterPlan",
    "activity_multiplier",
    "exponential_offsets",
    "plan_tick",
    "uniform_offsets",
]