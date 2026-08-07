"""Aplicacao FastAPI: monta os routers e gerencia o ciclo de vida.

DUAS RESPONSABILIDADES NUM PROCESSO

Este servico e ao mesmo tempo a API REST e o SCHEDULER. O dispatcher roda como
background task iniciada no lifespan (ADR-010).

O ganho e concreto: um container a menos, um alvo de build a menos, e o
dispatcher reaproveita o pool de conexoes e o publisher que a API ja mantem.

O custo tambem: um bug no loop do dispatcher pode degradar a API, e escalar a
API para varias replicas exigiria cuidado para nao materializar tarefas em
duplicidade. A segunda preocupacao ja esta tratada -- o
`CampaignRepository.claim_active_for_dispatch` usa `FOR UPDATE SKIP LOCKED`,
entao duas replicas nunca pegam a mesma campanha no mesmo tick.

ORDEM DO STARTUP

    1. logging          (para que os erros seguintes sejam legiveis)
    2. Postgres         (verifica que responde)
    3. Redis            (verifica que responde)
    4. RabbitMQ         (conecta o publisher e declara a topologia)
    5. dispatcher       (background task)

A ordem importa: o dispatcher publica no RabbitMQ e le do Postgres, entao subir
antes das dependencias so produziria excecoes no primeiro tick.

O startup NAO aborta se uma dependencia falhar. Em vez disso, registra o erro em
`state.startup_errors` e deixa `/health/ready` responder 503. Assim o container
sobe, fica observavel e volta ao normal sozinho quando a dependencia se recupera
-- em vez de entrar em CrashLoopBackOff, onde nao da nem para ler o log com
calma.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from apt import __version__
from apt.api.deps import AppState
from apt.api.routers import admin, campaigns, flags, health, platforms
from apt.config import get_settings
from apt.db.engine import check_health as db_health
from apt.db.engine import dispose_engine
from apt.logging_setup import (
    bind_correlation_id,
    configure_logging,
    get_correlation_id,
    get_logger,
)
from apt.messaging.publisher import close_publisher, get_publisher
from apt.resilience.redis_client import check_health as redis_health
from apt.resilience.redis_client import close_redis
from apt.scheduling.dispatcher import Dispatcher

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup e shutdown da aplicacao."""
    settings = get_settings()
    configure_logging(
        service_name=settings.service_name,
        level=settings.log_level,
        as_json=settings.log_json,
    )
    logger.info("api.starting", version=__version__, env=settings.env)

    state = AppState()
    app.state.apt = state

    # --- Dependencias ------------------------------------------------------
    if not await db_health():
        state.startup_errors.append("postgres indisponivel no startup")
        logger.error("api.startup_postgres_unavailable")

    if not await redis_health():
        state.startup_errors.append("redis indisponivel no startup")
        logger.error("api.startup_redis_unavailable")

    publisher = get_publisher()
    try:
        await publisher.connect()
    except Exception as exc:
        state.startup_errors.append(f"rabbitmq indisponivel: {exc}")
        logger.error("api.startup_rabbitmq_unavailable", error=str(exc))

    # --- Scheduler ---------------------------------------------------------
    # Sobe mesmo com erro de dependencia: o loop dele tem tratamento proprio de
    # excecao por tick e volta a funcionar quando a dependencia se recuperar.
    dispatcher = Dispatcher(publisher)
    state.dispatcher = dispatcher
    state.dispatcher_task = asyncio.create_task(dispatcher.run(), name="apt-dispatcher")
    state.ready = not state.startup_errors

    logger.info(
        "api.started",
        startup_errors=len(state.startup_errors),
        dispatcher="running",
    )

    try:
        yield
    finally:
        # --- Shutdown ------------------------------------------------------
        logger.info("api.stopping")

        dispatcher.stop()
        task = state.dispatcher_task
        if isinstance(task, asyncio.Task):
            try:
                # 5s e folgado: o dispatcher checa o evento de parada a cada
                # tick de 1s. O timeout existe para o caso de ele estar preso
                # numa consulta lenta ao banco.
                await asyncio.wait_for(task, timeout=5.0)
            except TimeoutError:
                logger.warning("api.dispatcher_shutdown_timeout")
                task.cancel()

        await close_publisher()
        await close_redis()
        await dispose_engine()
        logger.info("api.stopped")


def create_app() -> FastAPI:
    """Monta a aplicacao. Funcao factory para que os testes criem instancias isoladas."""
    app = FastAPI(
        title="Anti-Platform Throttling",
        version=__version__,
        description=(
            "POC 4 -- Engenharia de Sistemas Distribuidos (UFPB, 2026.1).\n\n"
            "Controla o envio de requisicoes a plataformas externas evitando "
            "rate limiting e throttling. Seis padroes arquiteturais: "
            "**Rate Limit** (token bucket distribuido em Redis), "
            "**Circuit Breaker** (estado compartilhado), "
            "**Queues/PubSub/Fanout + DLQ** (RabbitMQ), "
            "**Load Balancing** (competing consumers), "
            "**Bulkhead** (isolamento por plataforma) e "
            "**Feature Flag** (alteravel em runtime).\n\n"
            "Os thresholds das plataformas sao ESTIMATIVAS para ambiente "
            "controlado, nao limites oficiais -- ver `docs/adr/ADR-008`."
        ),
        lifespan=lifespan,
    )

    # --- Middleware de correlacao -----------------------------------------
    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        """Garante um `correlation_id` por requisicao e o devolve no header.

        Reaproveita `X-Correlation-ID` quando o cliente manda um -- e o que
        permite a um teste de carga correlacionar a sua propria requisicao com os
        logs da API e dos workers que a atenderam.
        """
        incoming = request.headers.get("X-Correlation-ID")
        cid = bind_correlation_id(incoming)
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = cid
        return response

    # --- Tratamento de erro nao previsto ----------------------------------
    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        """Converte excecao nao tratada em 500 com id de correlacao.

        Devolver o `correlation_id` no corpo e o que torna o erro investigavel:
        quem recebeu o 500 pode informar o id, e o log completo -- com stack
        trace -- e encontrado por ele. Sem isso, "deu erro 500" e uma queixa sem
        rastro.
        """
        cid = get_correlation_id()
        logger.exception(
            "api.unhandled_exception",
            path=request.url.path,
            method=request.method,
            error=str(exc),
        )
        return JSONResponse(
            status_code=500,
            content={
                "detail": "erro interno",
                "correlation_id": cid,
                "hint": "informe o correlation_id para localizar o log deste erro",
            },
        )

    # --- Routers ----------------------------------------------------------
    app.include_router(health.router)
    app.include_router(campaigns.router)
    app.include_router(platforms.router)
    app.include_router(flags.router)
    app.include_router(admin.router)

    @app.get("/", tags=["saude"], summary="Informacoes basicas do servico")
    async def root() -> dict[str, str]:
        return {
            "service": "anti-platform-throttling",
            "version": __version__,
            "docs": "/docs",
            "metrics": "/metrics",
            "health": "/health/ready",
        }

    return app


app = create_app()
