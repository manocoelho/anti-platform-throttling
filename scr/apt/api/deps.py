"""Dependencias e estado compartilhado da API.

O FastAPI resolve dependencias por chamada de endpoint. Aqui expomos os
componentes de longa vida (publisher, dispatcher, rate limiter, breaker, flags)
como funcoes de dependencia, para que os routers nao importem singletons
diretamente.

A vantagem pratica e testabilidade: um teste pode sobrescrever
`app.dependency_overrides[get_publisher_dep]` e injetar um duplo, sem monkeypatch
em variavel global de modulo.

`AppState` guarda o que e criado no lifespan e nao existe antes dele -- o
dispatcher, essencialmente. Ele nasce junto com a aplicacao e morre com ela.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status

from apt.messaging.publisher import Publisher, get_publisher
from apt.resilience.circuit_breaker import CircuitBreaker, get_circuit_breaker
from apt.resilience.feature_flags import FeatureFlags, get_feature_flags
from apt.resilience.rate_limiter import RateLimiter, get_rate_limiter
from apt.scheduling.dispatcher import Dispatcher


@dataclass(slots=True)
class AppState:
    """Objetos de longa vida da aplicacao, criados no lifespan.

    Guardado em `app.state.apt`. Nao usamos variaveis de modulo para estes
    porque eles tem ciclo de vida atrelado a aplicacao -- e, nos testes, varias
    instancias de app podem coexistir no mesmo processo.
    """

    dispatcher: Dispatcher | None = None
    dispatcher_task: object | None = None  # asyncio.Task; `object` evita import ciclico
    ready: bool = False
    startup_errors: list[str] = field(default_factory=list)


def get_state(request: Request) -> AppState:
    """Devolve o `AppState` da aplicacao atual."""
    state: AppState | None = getattr(request.app.state, "apt", None)
    if state is None:  # pragma: no cover - so ocorreria com lifespan desligado
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="aplicacao ainda inicializando",
        )
    return state


def get_publisher_dep() -> Publisher:
    """Publisher conectado ao RabbitMQ."""
    return get_publisher()


def get_rate_limiter_dep() -> RateLimiter:
    """Rate limiter distribuido."""
    return get_rate_limiter()


def get_breaker_dep() -> CircuitBreaker:
    """Circuit breaker distribuido."""
    return get_circuit_breaker(observer_id="api")


def get_flags_dep() -> FeatureFlags:
    """Feature flags."""
    return get_feature_flags()


# Aliases para as anotacoes ficarem curtas nas assinaturas dos endpoints.
StateDep = Annotated[AppState, Depends(get_state)]
PublisherDep = Annotated[Publisher, Depends(get_publisher_dep)]
RateLimiterDep = Annotated[RateLimiter, Depends(get_rate_limiter_dep)]
BreakerDep = Annotated[CircuitBreaker, Depends(get_breaker_dep)]
FlagsDep = Annotated[FeatureFlags, Depends(get_flags_dep)]
