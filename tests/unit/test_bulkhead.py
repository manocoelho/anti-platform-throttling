"""Testes do bulkhead: isolamento de concorrencia por plataforma.

Dois comportamentos importam mais que os outros:

1. ISOLAMENTO -- esgotar o compartimento do Instagram nao deve afetar o do
   YouTube. E a razao de existir do padrao.
2. NAO VAZAR SLOT NO TIMEOUT -- quando `wait_for` cancela um `acquire` pendente,
   o slot NAO pode ficar preso. Uma implementacao que vaza um slot por timeout
   estreitaria o compartimento progressivamente, e a plataforma acabaria sem
   atendimento por um bug que so aparece depois de horas de carga.
"""

from __future__ import annotations

import asyncio

import pytest

from apt.domain.models import Platform
from apt.resilience.bulkhead import Bulkhead, BulkheadRegistry


class TestBulkhead:
    async def test_permite_ate_a_capacidade(self) -> None:
        bh = Bulkhead(Platform.YOUTUBE, capacity=3, acquire_timeout=0.1)
        assert await bh.acquire() is True
        assert await bh.acquire() is True
        assert await bh.acquire() is True
        assert bh.stats.in_use == 3
        assert bh.available == 0

    async def test_recusa_quando_cheio_apos_o_timeout(self) -> None:
        """Sem slot, `acquire` devolve False em vez de esperar indefinidamente.

        Espera sem limite transformaria o semaforo numa fila invisivel: as
        tarefas nao apareceriam nem na fila do broker nem em execucao, a memoria
        cresceria em silencio e a latencia medida perderia significado.
        """
        bh = Bulkhead(Platform.INSTAGRAM, capacity=1, acquire_timeout=0.05)
        assert await bh.acquire() is True
        assert await bh.acquire() is False
        assert bh.stats.rejected_total == 1

    async def test_release_libera_o_slot(self) -> None:
        bh = Bulkhead(Platform.YOUTUBE, capacity=1, acquire_timeout=0.05)
        await bh.acquire()
        bh.release()
        assert bh.stats.in_use == 0
        assert await bh.acquire() is True

    async def test_timeout_nao_vaza_slot(self) -> None:
        """Apos varios timeouts, a capacidade original continua disponivel.

        Quando `asyncio.wait_for` estoura, ele CANCELA a corrotina do
        `Semaphore.acquire`. O `asyncio.Semaphore` trata o cancelamento
        corretamente e nao deixa um "acquire fantasma" pendente.

        Este teste guarda essa propriedade: uma implementacao caseira de semaforo
        (ou uma versao futura com bug) perderia um slot por timeout, e o
        compartimento se estreitaria com o tempo.
        """
        bh = Bulkhead(Platform.YOUTUBE, capacity=2, acquire_timeout=0.02)
        assert await bh.acquire() is True
        assert await bh.acquire() is True

        # Cinco tentativas que estouram o timeout.
        for _ in range(5):
            assert await bh.acquire() is False

        # Devolvemos os dois slots originais.
        bh.release()
        bh.release()
        assert bh.stats.in_use == 0

        # A capacidade continua sendo 2 -- nada vazou.
        assert await bh.acquire() is True
        assert await bh.acquire() is True
        assert await bh.acquire() is False

    async def test_release_em_excesso_nao_deixa_contador_negativo(self) -> None:
        """`release` sem `acquire` correspondente nao corrompe a metrica.

        O clamp existe porque um contador negativo faria a metrica de ocupacao
        mentir de forma persistente -- e a metrica errada e pior que a metrica
        ausente.
        """
        bh = Bulkhead(Platform.YOUTUBE, capacity=2, acquire_timeout=0.05)
        bh.release()
        bh.release()
        assert bh.stats.in_use == 0

    async def test_registra_o_pico_de_ocupacao(self) -> None:
        """`max_in_use` guarda o pico -- indica se a cota esta bem dimensionada."""
        bh = Bulkhead(Platform.YOUTUBE, capacity=4, acquire_timeout=0.05)
        await bh.acquire()
        await bh.acquire()
        await bh.acquire()
        bh.release()
        assert bh.stats.max_in_use == 3
        assert bh.stats.in_use == 2

    def test_capacidade_invalida_estoura(self) -> None:
        """Capacidade zero significaria nunca atender aquela plataforma."""
        with pytest.raises(ValueError):
            Bulkhead(Platform.YOUTUBE, capacity=0, acquire_timeout=0.05)


class TestIsolamento:
    """A propriedade central do padrao."""

    async def test_plataforma_esgotada_nao_afeta_a_outra(self) -> None:
        """Instagram lotado; YouTube continua atendendo normalmente.

        E o cenario que a demonstracao reproduz ao vivo: o Instagram degrada,
        esgota os slots dele, e os envios de YouTube seguem saindo. Com um pool
        compartilhado, os envios lentos de Instagram tomariam todos os slots e a
        vazao de YouTube cairia junto -- falha em cascata.
        """
        registry = BulkheadRegistry(
            compartments={
                Platform.INSTAGRAM: Bulkhead(Platform.INSTAGRAM, capacity=2, acquire_timeout=0.02),
                Platform.YOUTUBE: Bulkhead(Platform.YOUTUBE, capacity=4, acquire_timeout=0.02),
            }
        )

        instagram = registry.get(Platform.INSTAGRAM)
        youtube = registry.get(Platform.YOUTUBE)

        # Esgota o Instagram por completo.
        assert await instagram.acquire() is True
        assert await instagram.acquire() is True
        assert await instagram.acquire() is False

        # O YouTube nao foi afetado: os 4 slots dele estao livres.
        for _ in range(4):
            assert await youtube.acquire() is True
        assert youtube.stats.in_use == 4
        assert instagram.stats.rejected_total == 1

    async def test_carga_concorrente_respeita_a_capacidade(self) -> None:
        """Sob 20 corrotinas concorrentes, no maximo 3 entram ao mesmo tempo.

        Verifica o semaforo sob concorrencia real, e nao apenas em chamadas
        sequenciais.
        """
        bh = Bulkhead(Platform.YOUTUBE, capacity=3, acquire_timeout=1.0)
        observed_peak = 0

        async def worker() -> None:
            nonlocal observed_peak
            if await bh.acquire():
                try:
                    observed_peak = max(observed_peak, bh.stats.in_use)
                    await asyncio.sleep(0.01)
                finally:
                    bh.release()

        await asyncio.gather(*(worker() for _ in range(20)))
        assert observed_peak <= 3
        assert bh.stats.in_use == 0


class TestSnapshot:
    async def test_snapshot_expoe_os_contadores(self) -> None:
        registry = BulkheadRegistry(
            compartments={
                Platform.YOUTUBE: Bulkhead(Platform.YOUTUBE, capacity=2, acquire_timeout=0.02)
            }
        )
        await registry.get(Platform.YOUTUBE).acquire()
        snapshot = registry.snapshot()

        assert snapshot["youtube"]["capacity"] == 2
        assert snapshot["youtube"]["in_use"] == 1
        assert snapshot["youtube"]["available"] == 1

    def test_plataforma_sem_compartimento_estoura(self) -> None:
        """Enviar sem compartimento eliminaria o isolamento -- falhamos alto."""
        registry = BulkheadRegistry(compartments={})
        with pytest.raises(KeyError):
            registry.get(Platform.YOUTUBE)
