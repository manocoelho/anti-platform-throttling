"""Cliente Redis compartilhado e carregamento dos scripts Lua.

Tres componentes guardam estado no Redis -- rate limiter, circuit breaker e
feature flags -- e todos passam por aqui. Um pool de conexoes por processo, nao
um por componente: com tres pools, um worker abriria o triplo de conexoes sem
nenhum ganho.

Sobre os scripts Lua: eles sao registrados no Redis via `SCRIPT LOAD` e depois
invocados por `EVALSHA <hash>`, que manda apenas o hash de 40 caracteres em vez
do corpo inteiro do script a cada chamada. A biblioteca `redis-py` cuida do
detalhe importante: se o Redis reiniciar e perder o cache de scripts, ela
recebe o erro `NOSCRIPT`, reenvia o corpo automaticamente e repete a chamada.
"""

from __future__ import annotations

from importlib import resources

import redis.asyncio as aioredis
from redis.asyncio.client import Redis
from redis.commands.core import AsyncScript

from apt.config import get_settings
from apt.logging_setup import get_logger

logger = get_logger(__name__)

_client: Redis | None = None
# Cache dos scripts registrados, por nome de arquivo.
_scripts: dict[str, AsyncScript] = {}


def get_redis() -> Redis:
    """Devolve o cliente Redis do processo, criando-o na primeira chamada."""
    global _client
    if _client is None:
        settings = get_settings()
        _client = aioredis.from_url(
            settings.redis_url,
            # decode_responses=True faz o cliente devolver `str` em vez de
            # `bytes`. Simplifica o codigo das flags; os scripts Lua devolvem
            # numeros, entao a decodificacao nao os afeta.
            decode_responses=True,
            # O rate limiter esta no caminho critico de cada envio. Um Redis
            # que travou nao pode prender o worker: 2s de timeout garantem que
            # a chamada falha rapido e o codigo cai no comportamento de
            # degradacao (ver `RateLimiter.acquire`).
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
            # Detecta conexao morta sem depender do timeout do SO.
            health_check_interval=30,
            max_connections=50,
        )
        logger.info("redis.client_created", url=_redacted_url(settings.redis_url))
    return _client


def load_script(filename: str) -> AsyncScript:
    """Registra (uma vez) e devolve um script Lua de `resilience/lua/`.

    `importlib.resources` em vez de caminho relativo com `__file__`: assim o
    script e encontrado tanto rodando do codigo-fonte quanto do pacote
    instalado dentro do container -- onde a arvore de diretorios e diferente.
    O `pyproject.toml` declara os `.lua` como package-data justamente para
    isso.

    Args:
        filename: nome do arquivo, ex. "token_bucket.lua".
    """
    if filename in _scripts:
        return _scripts[filename]

    source = resources.files("apt.resilience.lua").joinpath(filename).read_text(encoding="utf-8")
    script = get_redis().register_script(source)
    _scripts[filename] = script
    logger.info("redis.script_registered", script=filename, bytes=len(source))
    return script


async def check_health() -> bool:
    """Testa se o Redis responde. Usado por `/health/ready`."""
    try:
        return bool(await get_redis().ping())
    except Exception as exc:
        logger.warning("redis.health_check_failed", error=str(exc))
        return False


async def close_redis() -> None:
    """Fecha o pool de conexoes. Chamado no shutdown dos servicos."""
    global _client, _scripts
    if _client is not None:
        await _client.aclose()
        logger.info("redis.client_closed")
        _client = None
        # Os scripts registrados apontam para o cliente antigo; descartamos
        # junto para que um novo `get_redis()` os registre no cliente novo.
        _scripts = {}


def _redacted_url(url: str) -> str:
    """Esconde a senha da URL antes de logar.

    Existe porque a URL de conexao aparece em log de boot, e log costuma ser
    enviado para agregadores externos. Vazar credencial em log e uma das formas
    mais comuns -- e mais silenciosas -- de exposicao de segredo.
    """
    if "@" not in url:
        return url
    scheme, _, rest = url.partition("://")
    _, _, host = rest.rpartition("@")
    return f"{scheme}://***@{host}"
