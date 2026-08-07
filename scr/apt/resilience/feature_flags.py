"""Feature flags com estado no Redis, cache local e invalidacao por fanout.

Para que servem aqui: o rate limiter e o circuit breaker sao mecanismos que
funcionam invisivelmente quando estao certos. As flags permitem DESLIGA-LOS em
runtime e mostrar, na mesma execucao, o que acontece sem eles. E o que
transforma a apresentacao de "confie em nos, esta funcionando" em "olhe o
grafico antes e depois".

Alem da demonstracao, elas cumprem o papel operacional classico do padrao:
mudar comportamento sem redeploy.

O PROBLEMA DE DESEMPENHO E COMO ELE E RESOLVIDO

Ler as flags no Redis a cada mensagem somaria uma ida a rede em cada envio --
no caminho critico, junto das duas leituras do rate limiter e uma do breaker.

Solucao: cache local com TTL curto (2s) + invalidacao ativa por fanout. Quando
alguem altera uma flag pela API, um evento `flags_changed` e publicado no
exchange fanout e TODOS os workers limpam o cache imediatamente. O TTL e apenas
a rede de seguranca para o caso do evento se perder.

Resultado: praticamente zero leituras de Redis no caminho critico, e propagacao
efetivamente instantanea quando a flag muda de verdade.

POR QUE FANOUT E NAO TOPIC

Com um exchange topic e uma fila compartilhada, o RabbitMQ entregaria o evento a
UM worker -- os outros seguiriam com o cache velho por ate 2 segundos. Fanout com
uma fila privada por worker garante que todos recebem. Ver
`messaging/topology.py`.
"""

from __future__ import annotations

import time
from typing import Final

from apt.logging_setup import get_logger
from apt.resilience.redis_client import get_redis

logger = get_logger(__name__)

_FLAGS_KEY: Final = "apt:flags"
# 2 segundos: curto o bastante para que uma flag alterada valha rapido mesmo se
# o evento de fanout se perder, longo o bastante para eliminar quase todas as
# leituras de Redis do caminho critico.
_CACHE_TTL_SECONDS: Final = 2.0


class Flag:
    """Nomes das flags. Constantes para evitar erro de digitacao em string solta."""

    # Desliga o rate limiter. A demonstracao mais direta da POC: com ele
    # desligado, os 429 aparecem em segundos.
    RATE_LIMITER_ENABLED = "rate_limiter_enabled"

    # Desliga o circuit breaker. Com ele desligado, o sistema insiste contra uma
    # plataforma fora do ar e a fila de retry cresce.
    CIRCUIT_BREAKER_ENABLED = "circuit_breaker_enabled"

    # Desliga o jitter na distribuicao temporal. Os envios passam a sair em
    # rajada no inicio de cada tick em vez de espalhados.
    JITTER_ENABLED = "jitter_enabled"

    # Pausa todas as campanhas automaticamente quando um circuito abre.
    # Comportamento defensivo, desligado por padrao: com ele ligado a POC
    # pararia de gerar trafego durante o teste de resiliencia, e nao daria para
    # observar o breaker sondando a recuperacao.
    AUTO_PAUSE_ON_OPEN = "auto_pause_on_open"

    # Desliga a materializacao de novas tarefas pelo scheduler, sem alterar o
    # status das campanhas. Util para drenar a fila antes de uma medicao.
    DISPATCH_ENABLED = "dispatch_enabled"


# Padroes. Todas as protecoes comecam LIGADAS -- se o Redis estiver vazio ou
# inacessivel, o sistema opera protegido. Uma flag ausente nunca deve significar
# "desligue a protecao".
DEFAULT_FLAGS: Final[dict[str, bool]] = {
    Flag.RATE_LIMITER_ENABLED: True,
    Flag.CIRCUIT_BREAKER_ENABLED: True,
    Flag.JITTER_ENABLED: True,
    Flag.AUTO_PAUSE_ON_OPEN: False,
    Flag.DISPATCH_ENABLED: True,
}


class FeatureFlags:
    """Leitura e escrita de flags, com cache local."""

    def __init__(self) -> None:
        self._cache: dict[str, bool] = dict(DEFAULT_FLAGS)
        # 0.0 forca a primeira leitura a ir ao Redis.
        self._cache_expires_at: float = 0.0

    async def _refresh(self) -> None:
        """Recarrega o cache a partir do Redis.

        Em caso de falha, MANTEM o cache atual em vez de voltar aos padroes.
        Isso importa: se um operador desligou o rate limiter deliberadamente e o
        Redis pisca, reverter para o padrao religaria o limiter no meio de uma
        operacao -- um comportamento surpresa, causado por uma falha de
        infraestrutura sem relacao com a decisao.
        """
        try:
            raw = await get_redis().hgetall(_FLAGS_KEY)
        except Exception as exc:
            logger.warning(
                "feature_flags.refresh_failed",
                error=str(exc),
                note="mantendo os valores em cache",
            )
            # Adia a proxima tentativa para nao martelar um Redis fora do ar a
            # cada mensagem processada.
            self._cache_expires_at = time.monotonic() + _CACHE_TTL_SECONDS
            return

        merged = dict(DEFAULT_FLAGS)
        for key, value in (raw or {}).items():
            # O Redis guarda tudo como texto. Aceitamos varias grafias porque a
            # chave pode ter sido escrita na mao com `redis-cli` durante a demo.
            merged[str(key)] = str(value).strip().lower() in {"1", "true", "yes", "on"}

        self._cache = merged
        self._cache_expires_at = time.monotonic() + _CACHE_TTL_SECONDS

    async def is_enabled(self, flag: str) -> bool:
        """Le uma flag, usando o cache quando ele esta valido."""
        if time.monotonic() >= self._cache_expires_at:
            await self._refresh()
        return self._cache.get(flag, DEFAULT_FLAGS.get(flag, False))

    async def all_flags(self) -> dict[str, bool]:
        """Todas as flags (forcando releitura). Usado pela API."""
        await self._refresh()
        return dict(self._cache)

    async def set(self, flag: str, value: bool) -> dict[str, bool]:
        """Grava uma flag no Redis e invalida o cache local.

        Nao publica o evento de fanout -- quem chama (o router da API) faz isso,
        porque so a API tem o publisher conectado. Separar as duas coisas evita
        que este modulo, usado tambem pelo worker, dependa de mensageria.

        Raises:
            KeyError: se a flag nao existe. Aceitar nome arbitrario permitiria
                criar uma flag por erro de digitacao, que ficaria eternamente
                sem efeito e sem ninguem perceber.
        """
        if flag not in DEFAULT_FLAGS:
            raise KeyError(f"flag desconhecida: '{flag}'. Validas: {sorted(DEFAULT_FLAGS)}")

        await get_redis().hset(_FLAGS_KEY, flag, "1" if value else "0")
        self.invalidate()
        logger.info("feature_flags.updated", flag=flag, value=value)
        return await self.all_flags()

    def invalidate(self) -> None:
        """Expira o cache local. Chamado ao receber `flags_changed` do fanout."""
        self._cache_expires_at = 0.0

    async def reset(self) -> dict[str, bool]:
        """Volta todas as flags aos padroes (apagando a chave no Redis)."""
        await get_redis().delete(_FLAGS_KEY)
        self.invalidate()
        logger.info("feature_flags.reset")
        return await self.all_flags()

    def cached(self) -> dict[str, bool]:
        """Valores em cache, sem tocar no Redis. Para logs e metricas."""
        return dict(self._cache)


_flags: FeatureFlags | None = None


def get_feature_flags() -> FeatureFlags:
    """Devolve as feature flags do processo.

    Uma instancia por processo -- e o que faz o cache valer a pena. Uma
    instancia por requisicao daria cache sempre frio e uma ida ao Redis por
    mensagem, anulando o mecanismo.
    """
    global _flags
    if _flags is None:
        _flags = FeatureFlags()
    return _flags