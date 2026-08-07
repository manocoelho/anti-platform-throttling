"""Testes de integracao do rate limiter distribuido (Redis + Lua).

O TESTE DE PARIDADE E O MAIS IMPORTANTE DESTE ARQUIVO.

O algoritmo do token bucket existe duas vezes no projeto: em Python
(`resilience/token_bucket.py`, a implementacao de referencia, testada
exaustivamente sem infraestrutura) e em Lua (`lua/token_bucket.lua`, que roda
atomicamente dentro do Redis). A duplicacao e deliberada e justificada -- ver o
docstring de `token_bucket.py`.

O risco dessa escolha e obvio: alguem corrige um bug numa implementacao e esquece
a outra. `test_paridade_com_a_implementacao_de_referencia` e a rede que pega
exatamente isso. Ele roda a MESMA sequencia de operacoes, com o MESMO timestamp
injetado, nas duas implementacoes, e compara os resultados.

O outro teste central e `test_concorrencia_nao_estoura_o_limite`: 50 corrotinas
disputando o mesmo bucket ao mesmo tempo. E a verificacao da atomicidade do
script Lua -- com read-modify-write comum, este teste falharia.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from apt.domain.models import Platform
from apt.resilience.rate_limiter import (
    content_bucket_key,
    get_rate_limiter,
    platform_bucket_key,
)
from apt.resilience.redis_client import load_script
from apt.resilience.token_bucket import BucketState, consume

pytestmark = pytest.mark.integration


class TestScriptLua:
    """Comportamento do script diretamente, sem a fachada."""

    async def test_bucket_novo_nasce_cheio(self, redis_client: object) -> None:
        script = load_script("token_bucket.lua")
        key = "apt:test:novo"
        await redis_client.delete(key)  # type: ignore[attr-defined]

        now_ms = int(time.time() * 1000)
        allowed, tokens_milli, retry_after = await script(
            keys=[key], args=[10.0, 10.0, now_ms, 1.0, 60]
        )

        assert int(allowed) == 1
        # 10 de capacidade - 1 consumida = 9, escalado por 1000.
        assert int(tokens_milli) == 9000
        assert int(retry_after) == 0

    async def test_nega_e_devolve_prazo_quando_esvazia(self, redis_client: object) -> None:
        script = load_script("token_bucket.lua")
        key = "apt:test:esvazia"
        await redis_client.delete(key)  # type: ignore[attr-defined]

        now_ms = int(time.time() * 1000)
        for _ in range(3):
            await script(keys=[key], args=[3.0, 3.0, now_ms, 1.0, 60])

        allowed, _tokens, retry_after = await script(keys=[key], args=[3.0, 3.0, now_ms, 1.0, 60])
        assert int(allowed) == 0
        assert int(retry_after) > 0

    async def test_capacidade_invalida_devolve_erro(self, redis_client: object) -> None:
        """`capacity=0` e erro de configuracao e o script recusa explicitamente.

        Sem a validacao, `refill_rps=0` causaria divisao por zero e o Redis
        devolveria `inf` -- que viajaria pelo sistema e explodiria num ponto
        distante da causa.
        """
        from redis.exceptions import ResponseError

        script = load_script("token_bucket.lua")
        now_ms = int(time.time() * 1000)
        with pytest.raises(ResponseError):
            await script(keys=["apt:test:invalido"], args=[0.0, 10.0, now_ms, 1.0, 60])

    async def test_ttl_e_aplicado(self, redis_client: object) -> None:
        """A chave recebe TTL -- e a faxina automatica dos buckets por conteudo.

        Sem TTL, o limite por URL acumularia uma chave por conteudo para sempre,
        inclusive de campanhas encerradas.
        """
        script = load_script("token_bucket.lua")
        key = "apt:test:ttl"
        await redis_client.delete(key)  # type: ignore[attr-defined]

        now_ms = int(time.time() * 1000)
        await script(keys=[key], args=[5.0, 5.0, now_ms, 1.0, 120])

        ttl = await redis_client.ttl(key)  # type: ignore[attr-defined]
        assert 0 < ttl <= 120


class TestParidade:
    """A garantia de que as duas implementacoes concordam."""

    async def test_paridade_com_a_implementacao_de_referencia(self, redis_client: object) -> None:
        """Mesma sequencia, mesmo timestamp, resultados identicos nas duas.

        Se alguem alterar o Lua sem alterar o Python (ou vice-versa), este teste
        quebra. E o que torna a duplicacao do algoritmo sustentavel.

        A tolerancia de 1ms no `retry_after` existe porque o Lua usa
        `math.floor` e o Python `int()` -- a diferenca maxima e de um
        milissegundo por arredondamento, e apertar isso tornaria o teste
        instavel sem ganho real.
        """
        script = load_script("token_bucket.lua")
        key = "apt:test:paridade"
        await redis_client.delete(key)  # type: ignore[attr-defined]

        capacity, refill_rps = 8.0, 4.0
        base_ms = int(time.time() * 1000)
        state: BucketState | None = None

        # 30 passos, avancando 100ms cada -- cobre esgotamento e refill parcial.
        for step in range(30):
            now_ms = base_ms + step * 100

            lua_allowed, lua_tokens_milli, lua_retry = await script(
                keys=[key], args=[capacity, refill_rps, now_ms, 1.0, 60]
            )
            lua_allowed = int(lua_allowed) == 1
            lua_tokens = int(lua_tokens_milli) / 1000.0
            lua_retry = int(lua_retry)

            decision = consume(state, capacity=capacity, refill_rps=refill_rps, now_ms=now_ms)
            state = decision.state

            assert lua_allowed == decision.allowed, (
                f"passo {step}: Lua permitiu={lua_allowed}, Python permitiu={decision.allowed}"
            )
            assert lua_tokens == pytest.approx(decision.tokens_remaining, abs=0.01), (
                f"passo {step}: fichas divergentes "
                f"(Lua={lua_tokens}, Python={decision.tokens_remaining})"
            )
            assert abs(lua_retry - decision.retry_after_ms) <= 1, (
                f"passo {step}: prazo divergente "
                f"(Lua={lua_retry}ms, Python={decision.retry_after_ms}ms)"
            )


class TestConcorrencia:
    """A propriedade que justifica o Lua."""

    async def test_concorrencia_nao_estoura_o_limite(self, redis_client: object) -> None:
        """50 corrotinas simultaneas: no maximo `capacity` sao permitidas.

        ESTE E O TESTE QUE JUSTIFICA A ESCOLHA DO SCRIPT LUA.

        Com a implementacao ingenua (GET, decide, SET), varias corrotinas leem o
        mesmo saldo antes de qualquer escrita, todas concluem que ha ficha, e
        passam mais requisicoes do que o orcamento permitia. A janela entre a
        leitura e a escrita e onde o limite se rompe -- e ela se abre justamente
        sob concorrencia, quando o limite mais importa.

        O script Lua roda atomicamente: enquanto executa, nenhum outro comando e
        processado pelo Redis. Nao existe janela.
        """
        script = load_script("token_bucket.lua")
        key = "apt:test:concorrencia"
        await redis_client.delete(key)  # type: ignore[attr-defined]

        capacity = 10.0
        # Timestamp FIXO para todas as chamadas: congela o refill e isola a
        # variavel sob teste. Sem isso, o tempo passando durante a execucao
        # reporia fichas e mais de 10 poderiam legitimamente passar.
        now_ms = int(time.time() * 1000)

        async def attempt() -> bool:
            allowed, _tokens, _retry = await script(
                keys=[key], args=[capacity, 1.0, now_ms, 1.0, 60]
            )
            return int(allowed) == 1

        results = await asyncio.gather(*(attempt() for _ in range(50)))
        permitidas = sum(results)

        assert permitidas == 10, (
            f"esperado exatamente 10 permitidas (a capacidade), obtido {permitidas}. "
            "Valor maior indica perda de atomicidade no script Lua."
        )


class TestRateLimiterFachada:
    """A fachada `RateLimiter`, com os dois eixos de limitacao."""

    async def test_permite_dentro_do_limite(self, clean_rate_limiter: None) -> None:
        limiter = get_rate_limiter()
        decision = await limiter.acquire(Platform.YOUTUBE, "https://exemplo/1")
        assert decision.allowed is True
        assert decision.limited_by is None

    async def test_eixo_do_conteudo_limita_url_unica(self, clean_rate_limiter: None) -> None:
        """Concentrar volume numa unica URL e barrado pelo eixo do conteudo.

        E a razao de existir do segundo eixo: sem ele, seria possivel enviar toda
        a cota da plataforma (16/s) para uma URL so -- dentro do limite agregado, e
        ainda assim um comportamento obviamente artificial.
        """
        limiter = get_rate_limiter()
        url = "https://exemplo/concentrada"

        allowed = 0
        limited_by: str | None = None
        # 30 tentativas na mesma URL. O limite por conteudo (4/s, burst 4) e bem
        # menor que o da plataforma (16/s), entao ele deve ser o gargalo.
        for _ in range(30):
            decision = await limiter.acquire(Platform.YOUTUBE, url)
            if decision.allowed:
                allowed += 1
            else:
                limited_by = decision.limited_by

        assert limited_by == "content", (
            "o eixo do conteudo deveria ser o gargalo numa URL unica, "
            f"mas quem limitou foi '{limited_by}'"
        )
        # Deve ficar na ordem do burst por conteudo, nao da cota da plataforma.
        assert allowed <= 8

    async def test_urls_distintas_nao_compartilham_bucket(self, clean_rate_limiter: None) -> None:
        """Distribuir entre varias URLs aumenta a vazao total aproveitavel.

        E a contrapartida do teste anterior, e a razao do pool de conteudos
        rotativos existir.
        """
        limiter = get_rate_limiter()
        allowed = 0
        for i in range(20):
            decision = await limiter.acquire(Platform.YOUTUBE, f"https://exemplo/{i}")
            if decision.allowed:
                allowed += 1

        # Com 20 URLs distintas, o eixo do conteudo nao e gargalo e o limite
        # efetivo passa a ser o da plataforma (burst 16).
        assert allowed >= 15

    async def test_plataformas_tem_buckets_independentes(self, clean_rate_limiter: None) -> None:
        """Esgotar o YouTube nao afeta o Instagram.

        Buckets por plataforma sao a base para o isolamento -- se compartilhassem
        um bucket, o volume de uma plataforma consumiria a cota da outra.
        """
        limiter = get_rate_limiter()

        for i in range(40):
            await limiter.acquire(Platform.YOUTUBE, f"https://yt/{i}")

        decision = await limiter.acquire(Platform.INSTAGRAM, "https://ig/1")
        assert decision.allowed is True

    async def test_peek_nao_consome_ficha(self, clean_rate_limiter: None) -> None:
        limiter = get_rate_limiter()
        antes = await limiter.peek(Platform.YOUTUBE)
        depois = await limiter.peek(Platform.YOUTUBE)
        assert antes is not None and depois is not None
        # `peek` faz o refill, entao o valor pode subir -- mas nunca cair.
        assert depois >= antes - 0.01

    async def test_reset_por_plataforma_apaga_so_o_bucket_alvo(
        self, clean_rate_limiter: None, redis_client: object
    ) -> None:
        limiter = get_rate_limiter()
        await limiter.acquire(Platform.YOUTUBE, "https://yt/x")
        await limiter.acquire(Platform.INSTAGRAM, "https://ig/x")

        await limiter.reset(Platform.YOUTUBE)

        assert await redis_client.exists(platform_bucket_key(Platform.YOUTUBE)) == 0  # type: ignore[attr-defined]
        assert await redis_client.exists(platform_bucket_key(Platform.INSTAGRAM)) == 1  # type: ignore[attr-defined]
        # O bucket por conteudo nao e alvo do reset por plataforma.
        assert await redis_client.exists(content_bucket_key("https://yt/x")) == 1  # type: ignore[attr-defined]
