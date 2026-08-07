"""Log estruturado com `correlation_id` propagado automaticamente.

Num sistema distribuido, um envio atravessa API -> RabbitMQ -> worker ->
plataforma. Sem um identificador comum, reconstruir o caminho de uma tarefa
significa cruzar timestamps de tres servicos na mao.

A solucao aqui e um `ContextVar` com o `correlation_id`: a API o gera por
requisicao, o dispatcher o grava na mensagem, o worker o restaura ao consumir.
Todo log emitido dentro daquele contexto carrega o campo automaticamente, sem
precisar passar o id explicitamente por parametro em cada funcao.

`ContextVar` (e nao uma variavel global) porque cada task do asyncio precisa da
sua propria copia: dois envios concorrentes no mesmo worker teriam ids
embaralhados com estado global.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import MutableMapping
from contextvars import ContextVar
from typing import Any
from uuid import uuid4

import structlog

# Vazio quando nao ha contexto (ex.: log de boot, antes de qualquer requisicao).
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def new_correlation_id() -> str:
    """Gera um id curto de correlacao.

    12 caracteres hex sao suficientes para nao colidir na escala da POC e sao
    bem mais legiveis num terminal que um UUID completo.
    """
    return uuid4().hex[:12]


def set_correlation_id(value: str) -> None:
    """Define o id de correlacao do contexto atual."""
    _correlation_id.set(value)


def get_correlation_id() -> str:
    """Le o id de correlacao do contexto atual (string vazia se nao houver)."""
    return _correlation_id.get()


def bind_correlation_id(value: str | None = None) -> str:
    """Garante um id no contexto e o devolve.

    Recebendo `None`, gera um novo. E o ponto de entrada usado tanto pelo
    middleware da API (que reaproveita o header `X-Correlation-ID` quando o
    cliente manda um) quanto pelo worker (que reaproveita o id vindo na
    mensagem).
    """
    resolved = value or new_correlation_id()
    set_correlation_id(resolved)
    return resolved


def _inject_correlation_id(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Processor do structlog que anexa o `correlation_id` a cada evento.

    A assinatura (`MutableMapping`, nao `dict`) e a que o structlog espera de um
    processor -- o event_dict passa por uma cadeia de processors e o tipo do
    contrato e o do protocolo, nao o da implementacao concreta.
    """
    cid = _correlation_id.get()
    if cid:
        event_dict["correlation_id"] = cid
    return event_dict


def configure_logging(service_name: str, level: str = "INFO", as_json: bool = True) -> None:
    """Configura o structlog para o processo atual.

    Deve ser chamada uma vez, no boot de cada servico (API, worker,
    platform-sim). Chamar mais de uma vez e inofensivo, mas desnecessario.

    Args:
        service_name: identifica a origem do log ("api", "worker", ...).
        level: nivel minimo ("DEBUG", "INFO", ...).
        as_json: True produz uma linha JSON por evento (bom para agregadores);
            False usa saida colorida e alinhada, melhor para ler no terminal
            durante a demo.
    """
    # Encaminha o logging da stdlib para o structlog. Sem isto, os logs do
    # uvicorn, do SQLAlchemy e do aio_pika sairiam num formato diferente.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if as_json
        else structlog.dev.ConsoleRenderer(colors=False)
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            _inject_correlation_id,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Amarra o nome do servico ao contexto global: todo log deste processo o
    # carrega, sem precisar repetir em cada chamada.
    structlog.contextvars.bind_contextvars(service=service_name)


def get_logger(name: str) -> Any:
    """Devolve um logger nomeado (normalmente `get_logger(__name__)`)."""
    return structlog.get_logger(name)
