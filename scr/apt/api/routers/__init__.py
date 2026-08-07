"""Routers da API, um modulo por area de responsabilidade.

campaigns  CRUD de campanhas, status consolidado, pausar/retomar
platforms  thresholds + estado ao vivo (fichas do balde, estado do circuito)
flags      feature flags alteraveis em runtime
admin      DLQ, eventos do breaker, metricas de teste, resets
health     liveness, readiness e /metrics
"""

from apt.api.routers import admin, campaigns, flags, health, platforms

__all__ = ["admin", "campaigns", "flags", "health", "platforms"]
