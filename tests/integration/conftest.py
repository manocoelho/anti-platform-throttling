"""Fixtures dos testes de integracao.

Estes testes EXIGEM a infraestrutura no ar. Localmente:

    docker compose up -d
    pytest tests/integration -v -m integration

No CI, os service containers do GitHub Actions fornecem Postgres, Redis e
RabbitMQ (ver `.github/workflows/ci.yml`).

Se a infraestrutura nao estiver disponivel, os testes fazem SKIP em vez de
falhar. A distincao importa: falha significa "o codigo esta errado", skip
significa "nao foi possivel verificar aqui". Confundir os dois treina a equipe a
ignorar vermelho.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest

# Nos testes de integracao apontamos para localhost -- as portas estao expostas
# pelo compose. `setdefault` para que o CI possa sobrescrever.
os.environ.setdefault(
    "APT_DATABASE_URL", "postgresql+asyncpg://apt:apt_local_password@localhost:5432/apt"
)
os.environ.setdefault("APT_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("APT_RABBITMQ_URL", "amqp://apt:apt_local_password@localhost:5672/")
os.environ.setdefault("APT_PLATFORM_SIM_URL", "http://localhost:9001")


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def redis_client() -> AsyncIterator[object]:
    """Cliente Redis, com skip automatico se o Redis nao responder."""
    from apt.resilience.redis_client import check_health, close_redis, get_redis

    if not await check_health():
        pytest.skip("Redis indisponivel -- suba o stack com `docker compose up -d`")

    yield get_redis()
    await close_redis()


@pytest.fixture
async def db_conn() -> AsyncIterator[object]:
    """Conexao com o Postgres, com skip automatico se o banco nao responder."""
    from apt.db.engine import check_health, connection, dispose_engine

    if not await check_health():
        pytest.skip("Postgres indisponivel -- suba o stack com `docker compose up -d`")

    async with connection() as conn:
        yield conn
    await dispose_engine()


@pytest.fixture
async def clean_rate_limiter() -> AsyncIterator[None]:
    """Zera os token buckets antes e depois do teste.

    Necessario porque o estado do rate limiter e COMPARTILHADO -- e essa e
    justamente a caracteristica que estamos testando. Sem a limpeza, um bucket
    drenado por um teste anterior faria o seguinte medir outra coisa.
    """
    from apt.resilience.rate_limiter import get_rate_limiter

    limiter = get_rate_limiter()
    try:
        await limiter.reset()
    except Exception:
        pytest.skip("Redis indisponivel -- suba o stack com `docker compose up -d`")

    yield
    await limiter.reset()


@pytest.fixture
async def clean_breaker() -> AsyncIterator[None]:
    """Fecha todos os circuitos antes e depois do teste."""
    from apt.resilience.circuit_breaker import get_circuit_breaker

    breaker = get_circuit_breaker(observer_id="test")
    try:
        await breaker.reset()
    except Exception:
        pytest.skip("Redis indisponivel -- suba o stack com `docker compose up -d`")

    yield
    await breaker.reset()
