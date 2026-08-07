"""Rate limiter distribuido -- o mecanismo central da POC.

O que "distribuido" significa aqui, concretamente: o estado do limite vive no
Redis, nao na memoria de cada worker. Consequencia pratica, e a tese que a
apresentacao demonstra ao vivo:

    subir de 1 para 5 workers NAO aumenta a vazao enviada a plataforma.

Com limiter em memoria de processo, cada worker teria o seu proprio balde de
16 req/s e cinco workers enviariam 80 req/s -- estourando o limite da plataforma
em 4x. O rate limiter existiria, estaria "funcionando" em cada processo, e o
sistema falharia justamente ao escalar.

DOIS EIXOS DE LIMITACAO

Cada envio consulta dois baldes e precisa de permissao dos dois:

    1. balde da PLATAFORMA  -- respeita o limite global (ex.: 16 req/s no YouTube)
    2. balde do CONTEUDO    -- limita cada URL (ex.: 4 req/s por URL)

O segundo eixo existe porque concentrar volume numa unica URL e o padrao que os
sistemas de deteccao procuram. Limitar apenas a plataforma permitiria 16 req/s
numa unica URL -- dentro do limite agregado, e ainda assim um comportamento
obviamente artificial.

O DETALHE DA ORDEM DE VERIFICACAO

A ordem e proposital: consultamos o balde do CONTEUDO primeiro (mais restritivo)
e o da PLATAFORMA depois. Se o conteudo nega, nunca tocamos o balde da
plataforma -- e portanto nao desperdicamos uma ficha da cota global numa
requisicao que nao vai sair.

Fazer o inverso vazaria fichas: consumiriamos da plataforma, o conteudo negaria,
o envio nao aconteceria, e a ficha ficaria perdida. Sob carga, esse vazamento
reduziria a vazao efetiva bem abaixo do configurado -- um bug de "esta mais
lento do que deveria" cuja causa e dificil de encontrar.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass

from apt.config import get_settings
from apt.domain.models import Platform
from apt.logging_setup import get_logger
from apt.resilience.redis_client import get_redis, load_script

logger = get_logger(__name__)

_SCRIPT_NAME = "token_bucket.lua"

# TTL das chaves de balde. Bem maior que qualquer intervalo de refill, para que
# um balde em uso jamais expire no meio da operacao; curto o bastante para que
# chaves de campanhas encerradas nao acumulem para sempre.
_BUCKET_TTL_SECONDS = 3600


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """Resultado consolidado de uma consulta ao rate limiter.

    Attributes:
        allowed: se o envio pode seguir.
        retry_after_ms: quando negado, quanto esperar. E o maior prazo entre os
            baldes consultados -- esperar menos que isso seria voltar cedo e ser
            negado de novo.
        limited_by: qual eixo negou ("platform", "content" ou None). Vai para a
            metrica: distinguir os dois e o que permite saber se o gargalo e o
            limite global ou a concentracao numa URL.
        platform_tokens: fichas restantes no balde da plataforma (observabilidade).
    """

    allowed: bool
    retry_after_ms: int = 0
    limited_by: str | None = None
    platform_tokens: float | None = None


def platform_bucket_key(platform: Platform) -> str:
    """Chave Redis do balde de uma plataforma."""
    return f"apt:rl:platform:{platform}"


def content_bucket_key(content_url: str) -> str:
    """Chave Redis do balde de um conteudo.

    A URL e hasheada antes de virar chave por tres razoes:

    1. URLs podem ser longas; chaves gigantes desperdicam memoria do Redis.
    2. URLs contem caracteres (`:`, espacos, query strings) que atrapalham a
       inspecao manual com `KEYS apt:rl:*`.
    3. Evita vazar a URL completa em ferramentas de monitoramento do Redis.

    SHA-256 truncado em 16 caracteres hex = 64 bits. A chance de colisao e
    desprezivel na escala da POC, e uma colisao apenas faria duas URLs
    compartilharem um balde -- o efeito seria um limite mais conservador, nunca
    mais permissivo.
    """
    digest = hashlib.sha256(content_url.encode("utf-8")).hexdigest()[:16]
    return f"apt:rl:content:{digest}"


class RateLimiter:
    """Fachada do token bucket distribuido."""

    def __init__(self) -> None:
        self._settings = get_settings()

    async def _consume(
        self, key: str, *, capacity: float, refill_rps: float, requested: float = 1.0
    ) -> tuple[bool, float, int]:
        """Executa o script Lua num balde.

        Returns:
            `(allowed, tokens_restantes, retry_after_ms)`.
        """
        script = load_script(_SCRIPT_NAME)
        now_ms = int(time.time() * 1000)

        raw = await script(
            keys=[key],
            args=[capacity, refill_rps, now_ms, requested, _BUCKET_TTL_SECONDS],
        )
        allowed_flag, tokens_milli, retry_after_ms = raw
        # O Lua devolve as fichas multiplicadas por 1000 para nao perder a parte
        # fracionaria na conversao do protocolo -- desfazemos aqui.
        return bool(int(allowed_flag)), int(tokens_milli) / 1000.0, int(retry_after_ms)

    async def acquire(self, platform: Platform, content_url: str) -> RateLimitDecision:
        """Pede permissao para um envio, consultando os dois eixos.

        Se o Redis estiver inacessivel, PERMITIMOS o envio (fail-open).

        Essa escolha merece justificativa, porque a alternativa e defensavel.
        Fail-closed (negar quando o Redis cai) protegeria o limite da plataforma
        de forma absoluta, mas pararia o sistema por inteiro: uma queda de Redis
        viraria indisponibilidade total, e as tarefas acumulariam na fila ate
        estourar o `x-max-length`.

        Escolhemos fail-open porque as outras camadas continuam ativas quando o
        Redis cai -- o bulkhead ainda limita a concorrencia por plataforma
        dentro de cada worker, e o circuit breaker (que tambem degrada, mas cujo
        estado local ainda observa 429) reage se a plataforma reclamar. Ou seja,
        perdemos a precisao do limite, nao todo o controle.

        A decisao esta registrada em docs/TRADE-OFFS.md, e o evento e logado em
        nivel ERROR com metrica propria justamente porque e um modo degradado
        que ninguem deveria descobrir por acaso.
        """
        rl = self._settings.rate_limit
        platform_rps, platform_burst = rl.for_platform(platform)

        try:
            # 1) Eixo do conteudo primeiro (mais restritivo). Negar aqui evita
            #    gastar ficha da cota global -- ver docstring do modulo.
            content_ok, _content_tokens, content_retry = await self._consume(
                content_bucket_key(content_url),
                capacity=float(rl.per_content_burst),
                refill_rps=rl.per_content_rps,
            )
            if not content_ok:
                return RateLimitDecision(
                    allowed=False,
                    retry_after_ms=content_retry,
                    limited_by="content",
                )

            # 2) Eixo da plataforma.
            platform_ok, platform_tokens, platform_retry = await self._consume(
                platform_bucket_key(platform),
                capacity=float(platform_burst),
                refill_rps=platform_rps,
            )
            if not platform_ok:
                # Nota conhecida: a ficha do conteudo ja foi consumida e nao e
                # devolvida. Preferimos assim a implementar compensacao -- o
                # efeito e o balde do conteudo ficar marginalmente mais
                # conservador, que e o lado seguro do erro.
                return RateLimitDecision(
                    allowed=False,
                    retry_after_ms=platform_retry,
                    limited_by="platform",
                    platform_tokens=platform_tokens,
                )

            return RateLimitDecision(allowed=True, platform_tokens=platform_tokens)

        except Exception as exc:
            logger.error(
                "rate_limiter.unavailable_fail_open",
                platform=str(platform),
                error=str(exc),
                note=(
                    "Redis inacessivel: permitindo o envio. O bulkhead e o "
                    "circuit breaker seguem ativos."
                ),
            )
            return RateLimitDecision(allowed=True, limited_by=None)

    async def peek(self, platform: Platform) -> float | None:
        """Le as fichas disponiveis de uma plataforma sem consumir nada.

        Consumindo `requested=0`, o script faz o refill e devolve o saldo sem
        debitar -- e a forma de observar o balde sem alterar o comportamento do
        sistema. Usado pela metrica de gauge e pelo endpoint de status.
        """
        rl = self._settings.rate_limit
        platform_rps, platform_burst = rl.for_platform(platform)
        try:
            _allowed, tokens, _retry = await self._consume(
                platform_bucket_key(platform),
                capacity=float(platform_burst),
                refill_rps=platform_rps,
                requested=0.0,
            )
            return tokens
        except Exception as exc:
            logger.warning("rate_limiter.peek_failed", error=str(exc))
            return None

    async def reset(self, platform: Platform | None = None) -> int:
        """Apaga baldes. Devolve quantas chaves foram removidas.

        Existe para os testes de carga: cada cenario precisa comecar com os
        baldes cheios, senao a sobra do teste anterior contaminaria a medicao.

        Sem `platform`, limpa tudo (inclusive os baldes por conteudo). Usamos
        `SCAN` e nao `KEYS`: `KEYS` percorre todo o keyspace num unico comando
        bloqueante -- num Redis com muitas chaves, isso congela o servidor por
        centenas de milissegundos e, como o rate limiter esta no caminho
        critico, congelaria o sistema inteiro junto.
        """
        client = get_redis()
        if platform is not None:
            return int(await client.delete(platform_bucket_key(platform)))

        removed = 0
        async for key in client.scan_iter(match="apt:rl:*", count=200):
            removed += int(await client.delete(key))
        logger.info("rate_limiter.reset", keys_removed=removed)
        return removed


_limiter: RateLimiter | None = None


def get_rate_limiter() -> RateLimiter:
    """Devolve o rate limiter do processo."""
    global _limiter
    if _limiter is None:
        _limiter = RateLimiter()
    return _limiter
