"""Testes de integracao do circuit breaker distribuido.

O teste central e `test_falhas_contadas_coletivamente`. Ele verifica a
propriedade que distingue este circuit breaker de qualquer implementacao de
biblioteca: as falhas sao contadas ENTRE PROCESSOS.

Com um breaker por processo e `failure_threshold=5`, cinco workers precisariam
observar 5 falhas cada -- 25 requisicoes contra uma plataforma ja em problema
antes do primeiro circuito abrir. E no caso de throttling, cada requisicao extra
durante a penalidade tende a estender a penalidade: o breaker por processo
produziria exatamente o comportamento que a POC existe para evitar.
"""

from __future__ import annotations

import asyncio

import pytest

from apt.config import get_settings
from apt.domain.models import BreakerState, Platform
from apt.resilience.circuit_breaker import CircuitBreaker, get_circuit_breaker

pytestmark = pytest.mark.integration


class TestTransicoes:
    async def test_comeca_fechado(self, clean_breaker: None) -> None:
        breaker = get_circuit_breaker(observer_id="test")
        decision = await breaker.allow(Platform.YOUTUBE)
        assert decision.allowed is True
        assert decision.state is BreakerState.CLOSED

    async def test_falhas_consecutivas_abrem(self, clean_breaker: None) -> None:
        breaker = get_circuit_breaker(observer_id="test")
        threshold = get_settings().circuit_breaker.failure_threshold

        for _ in range(threshold):
            await breaker.record_failure(Platform.YOUTUBE, reason="teste")

        decision = await breaker.allow(Platform.YOUTUBE)
        assert decision.allowed is False
        assert decision.state is BreakerState.OPEN
        assert decision.retry_after_ms > 0

    async def test_sucesso_zera_o_contador(self, clean_breaker: None) -> None:
        """Falhas nao consecutivas nao abrem o circuito."""
        breaker = get_circuit_breaker(observer_id="test")
        threshold = get_settings().circuit_breaker.failure_threshold

        for _ in range(threshold - 1):
            await breaker.record_failure(Platform.YOUTUBE, reason="teste")
        await breaker.record_success(Platform.YOUTUBE)
        for _ in range(threshold - 1):
            await breaker.record_failure(Platform.YOUTUBE, reason="teste")

        decision = await breaker.allow(Platform.YOUTUBE)
        assert decision.allowed is True
        assert decision.state is BreakerState.CLOSED

    async def test_registra_a_transicao(self, clean_breaker: None) -> None:
        """A abertura vem sinalizada como transicao, nao apenas como estado.

        E o que permite ao worker gravar a linha em `breaker_events` e emitir o
        log de WARNING -- exatamente uma vez, no momento da mudanca, em vez de a
        cada consulta.
        """
        breaker = get_circuit_breaker(observer_id="test")
        threshold = get_settings().circuit_breaker.failure_threshold

        transicoes = []
        for _ in range(threshold):
            decision = await breaker.record_failure(Platform.YOUTUBE, reason="teste")
            if decision.transition:
                transicoes.append(decision.transition)

        assert transicoes == [(BreakerState.CLOSED, BreakerState.OPEN)]


class TestEstadoCompartilhado:
    """A propriedade que justifica guardar o estado no Redis."""

    async def test_falhas_contadas_coletivamente(self, clean_breaker: None) -> None:
        """Cinco "workers" diferentes, uma falha cada: o circuito abre.

        ESTE TESTE E A JUSTIFICATIVA DO BREAKER DISTRIBUIDO.

        Cada `CircuitBreaker` aqui simula um processo distinto -- instancias
        separadas, com `observer_id` diferente, como se fossem cinco containers.
        Cada um registra UMA unica falha.

        Com estado por processo, nenhum deles chegaria perto do threshold de 5 e
        os cinco continuariam enviando. Como o estado esta no Redis, a quinta
        falha -- vista pelo quinto worker -- abre o circuito para TODOS.
        """
        threshold = get_settings().circuit_breaker.failure_threshold
        workers = [CircuitBreaker(observer_id=f"worker-{i}") for i in range(threshold)]

        for worker in workers:
            await worker.record_failure(Platform.YOUTUBE, reason="falha coletiva")

        # Um sexto worker, que nunca viu falha nenhuma, ja encontra o circuito aberto.
        novato = CircuitBreaker(observer_id="worker-novato")
        decision = await novato.allow(Platform.YOUTUBE)
        assert decision.allowed is False
        assert decision.state is BreakerState.OPEN

    async def test_circuitos_por_plataforma_sao_independentes(self, clean_breaker: None) -> None:
        """Abrir o circuito do Instagram nao afeta o do YouTube.

        E a juncao do Circuit Breaker com o Bulkhead, e o cenario que a
        demonstracao reproduz: o Instagram cai, o circuito dele abre, e o YouTube
        segue enviando na vazao normal.

        Um circuito unico compartilhado transformaria a falha de uma plataforma em
        parada total -- a falha em cascata que o padrao existe para impedir.
        """
        breaker = get_circuit_breaker(observer_id="test")
        threshold = get_settings().circuit_breaker.failure_threshold

        for _ in range(threshold):
            await breaker.record_failure(Platform.INSTAGRAM, reason="instagram fora")

        assert (await breaker.allow(Platform.INSTAGRAM)).allowed is False
        assert (await breaker.allow(Platform.YOUTUBE)).allowed is True

    async def test_concorrencia_nao_perde_contagem(self, clean_breaker: None) -> None:
        """Falhas simultaneas sao todas contadas.

        Sem atomicidade, dois processos leem `failure_count = 4`, ambos
        incrementam, ambos gravam 5 -- e uma falha se perde. O script Lua elimina
        a janela.
        """
        threshold = get_settings().circuit_breaker.failure_threshold
        workers = [CircuitBreaker(observer_id=f"w{i}") for i in range(threshold)]

        await asyncio.gather(
            *(w.record_failure(Platform.YOUTUBE, reason="concorrente") for w in workers)
        )

        snapshot = await get_circuit_breaker(observer_id="test").snapshot(Platform.YOUTUBE)
        assert snapshot["state"] == str(BreakerState.OPEN)


class TestSnapshotEReset:
    async def test_snapshot_de_circuito_inexistente_reporta_fechado(
        self, clean_breaker: None
    ) -> None:
        """Ausencia de informacao = permitir. E o default seguro."""
        breaker = get_circuit_breaker(observer_id="test")
        snapshot = await breaker.snapshot(Platform.YOUTUBE)
        assert snapshot["state"] == str(BreakerState.CLOSED)
        assert snapshot["failure_count"] == 0

    async def test_snapshot_expoe_o_contador(self, clean_breaker: None) -> None:
        breaker = get_circuit_breaker(observer_id="test")
        await breaker.record_failure(Platform.YOUTUBE, reason="teste")
        await breaker.record_failure(Platform.YOUTUBE, reason="teste")

        snapshot = await breaker.snapshot(Platform.YOUTUBE)
        assert snapshot["failure_count"] == 2
        assert snapshot["state"] == str(BreakerState.CLOSED)

    async def test_reset_fecha_o_circuito(self, clean_breaker: None) -> None:
        """Reset e a saida manual para quem SABE que a plataforma voltou."""
        breaker = get_circuit_breaker(observer_id="test")
        threshold = get_settings().circuit_breaker.failure_threshold

        for _ in range(threshold):
            await breaker.record_failure(Platform.YOUTUBE, reason="teste")
        assert (await breaker.allow(Platform.YOUTUBE)).allowed is False

        await breaker.reset(Platform.YOUTUBE)
        assert (await breaker.allow(Platform.YOUTUBE)).allowed is True

    async def test_reset_por_plataforma_nao_afeta_a_outra(self, clean_breaker: None) -> None:
        breaker = get_circuit_breaker(observer_id="test")
        threshold = get_settings().circuit_breaker.failure_threshold

        for platform in (Platform.YOUTUBE, Platform.INSTAGRAM):
            for _ in range(threshold):
                await breaker.record_failure(platform, reason="teste")

        await breaker.reset(Platform.YOUTUBE)

        assert (await breaker.allow(Platform.YOUTUBE)).allowed is True
        assert (await breaker.allow(Platform.INSTAGRAM)).allowed is False
