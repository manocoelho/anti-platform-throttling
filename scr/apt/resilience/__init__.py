"""Nucleo de resiliencia -- onde a POC realmente acontece.

Quatro dos seis padroes arquiteturais do projeto vivem aqui:

    rate_limiter.py     Rate Limit / Throttling -- token bucket distribuido
                        (Redis + Lua). O mecanismo central: o limite e GLOBAL,
                        entao escalar workers nao aumenta a vazao enviada.
    circuit_breaker.py  Circuit Breaker -- estado compartilhado no Redis, um
                        circuito por plataforma.
    bulkhead.py         Bulkhead / Isolation -- cota de concorrencia por
                        plataforma; uma degradada nao afeta a outra.
    feature_flags.py    Feature Flag -- liga/desliga protecoes em runtime.

Mais a politica de retry (`retry.py`), que completa o padrao Queues + DLQ junto
com `messaging/topology.py`.

Cada mecanismo distribuido aparece em duas formas:

    <nome>_state.py / token_bucket.py   implementacao PURA de referencia,
                                        testavel sem infraestrutura
    lua/<nome>.lua                      execucao ATOMICA no Redis

A razao da duplicacao esta no docstring de `token_bucket.py`, e a paridade entre
as duas e verificada por teste.
"""

from apt.resilience.breaker_state import (
    BreakerConfig,
    BreakerSnapshot,
    evaluate_allow,
    evaluate_failure,
    evaluate_success,
)
from apt.resilience.bulkhead import Bulkhead, BulkheadRegistry, BulkheadStats
from apt.resilience.circuit_breaker import (
    BreakerDecision,
    CircuitBreaker,
    get_circuit_breaker,
)
from apt.resilience.feature_flags import (
    DEFAULT_FLAGS,
    FeatureFlags,
    Flag,
    get_feature_flags,
)
from apt.resilience.rate_limiter import (
    RateLimitDecision,
    RateLimiter,
    get_rate_limiter,
)
from apt.resilience.redis_client import close_redis, get_redis
from apt.resilience.retry import (
    backoff_ms,
    choose_tier,
    is_retryable_status,
    tier_for_attempt,
    tier_for_retry_after,
)
from apt.resilience.token_bucket import BucketDecision, BucketState, consume, refill

__all__ = [
    "DEFAULT_FLAGS",
    "BreakerConfig",
    "BreakerDecision",
    "BreakerSnapshot",
    "BucketDecision",
    "BucketState",
    "Bulkhead",
    "BulkheadRegistry",
    "BulkheadStats",
    "CircuitBreaker",
    "FeatureFlags",
    "Flag",
    "RateLimitDecision",
    "RateLimiter",
    "backoff_ms",
    "choose_tier",
    "close_redis",
    "consume",
    "evaluate_allow",
    "evaluate_failure",
    "evaluate_success",
    "get_circuit_breaker",
    "get_feature_flags",
    "get_rate_limiter",
    "get_redis",
    "is_retryable_status",
    "refill",
    "tier_for_attempt",
    "tier_for_retry_after",
]
