"""Engine e sessoes do SQLAlchemy (modo assincrono).

Usamos SQLAlchemy Core -- `text()` com SQL explicito -- em vez do ORM. A razao
esta no ADR-012, resumida: as consultas do sistema sao poucas e algumas
precisam de recursos especificos do Postgres (`UPDATE ... RETURNING`,
`INSERT ... ON CONFLICT`, agregacoes com percentil). Com o ORM, boa parte delas
viraria `session.execute(text(...))` de qualquer forma, e ainda pagariamos o
custo de manter modelos declarativos duplicando o schema SQL.

Tudo e assincrono porque o worker e a API sao I/O-bound: enquanto uma tarefa
espera o Postgres, o event loop atende outra.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)

from apt.config import get_settings
from apt.logging_setup import get_logger

logger = get_logger(__name__)

# Engine unico por processo. O SQLAlchemy mantem o pool de conexoes dentro dele,
# entao criar um engine por requisicao (erro comum) abriria e fecharia conexao
# TCP com o Postgres a cada chamada.
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker | None = None


def get_engine() -> AsyncEngine:
    """Devolve o engine do processo, criando-o na primeira chamada."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.database_url,
            # 10 conexoes por processo. Com a API + 5 workers, chegamos a 60
            # conexoes -- confortavelmente abaixo do max_connections=100 padrao
            # do Postgres.
            pool_size=10,
            max_overflow=5,
            # pool_pre_ping faz um SELECT 1 antes de entregar a conexao. Custa
            # uma ida ao banco, mas evita o erro classico de pegar do pool uma
            # conexao que o Postgres ja fechou (acontece sempre que se reinicia
            # o container do banco com o stack no ar).
            pool_pre_ping=True,
            # Recicla conexoes de mais de 30 min, antes que algum firewall ou
            # timeout de rede as derrube silenciosamente.
            pool_recycle=1800,
            echo=False,
        )
        logger.info("db.engine_created", pool_size=10)
    return _engine


def get_session_factory() -> async_sessionmaker:
    """Devolve a factory de sessoes do processo."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=get_engine(),
            expire_on_commit=False,
            autoflush=False,
        )
    return _session_factory


@asynccontextmanager
async def connection() -> AsyncIterator[AsyncConnection]:
    """Conexao com transacao gerenciada: commit no fim, rollback em excecao.

    Uso:
        async with connection() as conn:
            await conn.execute(text("INSERT ..."), params)
        # commit ja aconteceu aqui

    `engine.begin()` faz o commit ao sair do bloco sem erro e o rollback se uma
    excecao propagar. Isso remove a classe de bug mais comum desta camada:
    esquecer o commit e ver a escrita desaparecer sem nenhum erro visivel.
    """
    engine = get_engine()
    async with engine.begin() as conn:
        yield conn


async def check_health() -> bool:
    """Testa se o banco responde. Usado pelo endpoint `/health/ready`.

    Devolve False em vez de propagar a excecao: um health check tem de
    responder "nao estou pronto", nao explodir com 500.
    """
    from sqlalchemy import text

    try:
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.warning("db.health_check_failed", error=str(exc))
        return False


async def dispose_engine() -> None:
    """Fecha o pool de conexoes. Chamado no shutdown dos servicos.

    Sem isso, o Postgres mantem as conexoes abertas ate o timeout dele e o
    `docker compose down` fica lento sem motivo aparente.
    """
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        logger.info("db.engine_disposed")
        _engine = None
        _session_factory = None
