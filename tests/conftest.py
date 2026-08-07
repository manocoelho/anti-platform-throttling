"""Configuracao compartilhada do pytest.

Os testes unitarios rodam SEM infraestrutura -- e um requisito de projeto, nao um
acidente. A logica que precisa ser verificada exaustivamente (token bucket,
maquina de estados do breaker, jitter, backoff) foi escrita como funcao pura
exatamente para permitir isso. Ver o docstring de
`apt/resilience/token_bucket.py`.

Consequencia pratica: `pytest tests/unit` roda em segundos, no CI, sem levantar
Postgres, Redis nem RabbitMQ.
"""

from __future__ import annotations

import os
import random

import pytest

# Configuracao previsivel para os testes. Definimos ANTES de qualquer import de
# `apt.config`, porque `get_settings()` tem cache -- se algum modulo ler as
# settings primeiro, os valores abaixo nao teriam efeito.
os.environ.setdefault("APT_ENV", "test")
os.environ.setdefault("APT_LOG_LEVEL", "WARNING")
os.environ.setdefault("APT_LOG_JSON", "false")
os.environ.setdefault("APT_RETRY_BASE_MS", "500")
os.environ.setdefault("APT_RETRY_MAX_MS", "120000")


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> None:
    """Limpa o cache de configuracao entre testes.

    `get_settings()` usa `lru_cache`. Um teste que altera variavel de ambiente
    contaminaria os seguintes se o cache persistisse -- e o tipo de acoplamento
    que faz a suite passar isolada e falhar completa.
    """
    from apt.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def rng() -> random.Random:
    """Gerador aleatorio com semente fixa.

    Todo modulo que sorteia (jitter, backoff) aceita um `Random` injetado
    justamente para que os testes sejam deterministicos. Sem isso, um teste que
    verifica distribuicao estatistica falharia esporadicamente -- e um teste que
    falha 1 em 50 execucoes e pior que nenhum teste, porque treina a equipe a
    ignorar vermelho.
    """
    return random.Random(20260807)


@pytest.fixture
def now_ms() -> int:
    """Instante fixo de referencia, em epoch de milissegundos.

    Valor arbitrario mas constante. Todas as funcoes de tempo do projeto recebem
    `now_ms` como parametro (em vez de chamar o relogio internamente), o que
    permite testar transicoes que dependem de tempo -- como OPEN -> HALF_OPEN
    apos 15 segundos -- sem esperar 15 segundos de verdade.
    """
    return 1_770_000_000_000
