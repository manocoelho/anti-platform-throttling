"""Observabilidade: metricas Prometheus.

Todas as metricas do sistema sao declaradas em `metrics.py` -- ver o docstring
daquele modulo para a convencao de nomes e a nota sobre cardinalidade de labels.

Os tres processos Python expoem `/metrics`: API (porta 8000), worker (9100) e
simulador (9001). O Prometheus descobre as replicas do worker pelo DNS do
Docker Compose (`prometheus/prometheus.yml`).
"""

from apt.observability import metrics
from apt.observability.metrics import CONTENT_TYPE, render_metrics, set_circuit_state

__all__ = ["CONTENT_TYPE", "metrics", "render_metrics", "set_circuit_state"]
