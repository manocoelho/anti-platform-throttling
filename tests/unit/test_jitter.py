"""Testes da distribuicao temporal (jitter).

Testar codigo aleatorio exige cuidado. A abordagem aqui e verificar
PROPRIEDADES em vez de valores exatos, com um `Random` de semente fixa injetado
para que a suite seja deterministica:

- os offsets saem ordenados (o dispatcher publica na ordem que recebe);
- a media dos intervalos fica proxima do alvo;
- o perfil diario modula o volume na direcao esperada;
- com `jitter_enabled=False`, o comportamento vira rajada -- o que a
  demonstracao usa para provocar 429 de proposito.
"""

from __future__ import annotations

import itertools
import random
import statistics

import pytest

from apt.domain.models import JitterStrategy
from apt.scheduling.jitter import (
    HOURLY_ACTIVITY_PROFILE,
    activity_multiplier,
    exponential_offsets,
    plan_tick,
    uniform_offsets,
)


class TestPerfilDiario:
    """Modulacao do volume ao longo do dia."""

    def test_perfil_cobre_as_24_horas(self) -> None:
        assert len(HOURLY_ACTIVITY_PROFILE) == 24

    def test_madrugada_tem_menos_atividade_que_a_noite(self) -> None:
        """3h da manha deve ter atividade bem menor que 19h.

        E a propriedade que torna o padrao "humanizado": volume constante ao
        longo de 24h e, por si so, um sinal artificial -- nenhuma audiencia real
        se comporta assim.
        """
        assert activity_multiplier(3) < activity_multiplier(19)

    def test_piso_evita_parada_total(self) -> None:
        """Nenhuma hora tem multiplicador abaixo do piso de 0.15.

        Sem o piso, a hora de menor atividade (0.12) faria o intervalo entre
        envios crescer mais de 8x e a campanha praticamente parar de madrugada --
        atrasando o orcamento total de forma que nao daria para compensar.
        """
        for hour in range(24):
            assert activity_multiplier(hour) >= 0.15

    @pytest.mark.parametrize("hour", [-1, 24, 100])
    def test_hora_invalida_estoura(self, hour: int) -> None:
        """Indice fora da faixa e erro, nao silencio.

        Um indice errado pegaria o perfil de outra hora e distorceria a
        distribuicao sem nenhum sinal visivel.
        """
        with pytest.raises(ValueError):
            activity_multiplier(hour)


class TestOffsetsUniformes:
    def test_offsets_ordenados_e_dentro_da_janela(self, rng: random.Random) -> None:
        offsets = uniform_offsets(20, window_ms=1000, rng=rng)
        assert len(offsets) == 20
        assert list(offsets) == sorted(offsets)
        assert all(0 <= o <= 1000 for o in offsets)

    def test_count_zero_devolve_vazio(self, rng: random.Random) -> None:
        assert uniform_offsets(0, window_ms=1000, rng=rng) == ()


class TestOffsetsExponenciais:
    def test_media_dos_intervalos_proxima_do_alvo(self, rng: random.Random) -> None:
        """Com amostra grande, a media dos intervalos converge para o alvo.

        Tolerancia de 15%: a distribuicao exponencial tem variancia alta por
        natureza, e apertar a tolerancia produziria um teste instavel -- que e
        pior que nenhum teste, porque treina a equipe a ignorar vermelho.
        """
        alvo = 50.0
        offsets = exponential_offsets(2000, mean_interval_ms=alvo, rng=rng)
        intervals = [b - a for a, b in itertools.pairwise(offsets)]
        assert statistics.mean(intervals) == pytest.approx(alvo, rel=0.15)

    def test_offsets_sao_monotonicos(self, rng: random.Random) -> None:
        """Sao soma acumulada de intervalos positivos, logo nao decrescem.

        Importa porque o dispatcher publica na ordem em que recebe: offsets fora
        de ordem fariam uma tarefa com `scheduled_at` posterior chegar antes, e os
        numeros de atraso no relatorio perderiam sentido.
        """
        offsets = exponential_offsets(200, mean_interval_ms=25.0, rng=rng)
        assert list(offsets) == sorted(offsets)

    def test_intervalo_medio_invalido_estoura(self, rng: random.Random) -> None:
        with pytest.raises(ValueError):
            exponential_offsets(10, mean_interval_ms=0.0, rng=rng)


class TestPlanTick:
    """Planejamento de um tick do dispatcher."""

    def test_respeita_o_orcamento_restante(self, rng: random.Random) -> None:
        """Nunca planeja mais que o que a campanha ainda pode enviar."""
        plan = plan_tick(
            strategy=JitterStrategy.UNIFORM,
            target_rate_per_min=6000,  # 100/s: muito acima do restante
            tick_seconds=1.0,
            hour_utc=12,
            remaining_budget=7,
            max_batch=200,
            rng=rng,
        )
        assert plan.count == 7

    def test_respeita_o_teto_de_batch(self, rng: random.Random) -> None:
        """`max_batch` e um teto de seguranca contra configuracao absurda."""
        plan = plan_tick(
            strategy=JitterStrategy.UNIFORM,
            target_rate_per_min=600_000,
            tick_seconds=1.0,
            hour_utc=12,
            remaining_budget=1_000_000,
            max_batch=50,
            rng=rng,
        )
        assert plan.count == 50

    def test_orcamento_esgotado_planeja_nada(self, rng: random.Random) -> None:
        plan = plan_tick(
            strategy=JitterStrategy.UNIFORM,
            target_rate_per_min=600,
            tick_seconds=1.0,
            hour_utc=12,
            remaining_budget=0,
            max_batch=200,
            rng=rng,
        )
        assert plan.count == 0
        assert plan.offsets_ms == ()

    def test_taxa_fracionaria_converge_na_media(self, rng: random.Random) -> None:
        """Vazao que da menos de 1 tarefa por tick ainda produz envios.

        Com 30/min e tick de 1s, o alvo e 0.5 tarefa por tick. Truncar levaria a
        zero para sempre e a campanha nunca sairia; arredondar sempre para cima
        entregaria o dobro do alvo. A parte fracionaria e tratada como
        PROBABILIDADE, e ao longo de muitos ticks a media converge.
        """
        total = sum(
            plan_tick(
                strategy=JitterStrategy.UNIFORM,
                target_rate_per_min=30,
                tick_seconds=1.0,
                hour_utc=12,
                remaining_budget=10_000,
                max_batch=200,
                rng=rng,
            ).count
            for _ in range(2000)
        )
        # Esperado ~1000 (0.5 por tick x 2000 ticks). Tolerancia de 12%.
        assert total == pytest.approx(1000, rel=0.12)

    def test_sem_jitter_produz_rajada(self, rng: random.Random) -> None:
        """`jitter_enabled=False` coloca todos os offsets em zero.

        E o modo controlado pela feature flag `jitter_enabled`, usado na
        demonstracao: as tarefas saem todas no inicio do tick, a rajada estoura a
        janela do simulador e os 429 aparecem.
        """
        plan = plan_tick(
            strategy=JitterStrategy.HUMANIZED,
            target_rate_per_min=1200,
            tick_seconds=1.0,
            hour_utc=12,
            remaining_budget=1000,
            max_batch=200,
            jitter_enabled=False,
            rng=rng,
        )
        assert plan.count > 0
        assert set(plan.offsets_ms) == {0}
        assert plan.mean_interval_ms == 0.0

    def test_com_jitter_espalha_os_offsets(self, rng: random.Random) -> None:
        """Com jitter, os offsets ocupam a janela em vez de se concentrarem."""
        plan = plan_tick(
            strategy=JitterStrategy.HUMANIZED,
            target_rate_per_min=1200,  # 20/s
            tick_seconds=1.0,
            hour_utc=12,
            remaining_budget=1000,
            max_batch=200,
            jitter_enabled=True,
            rng=rng,
        )
        assert plan.count > 1
        assert len(set(plan.offsets_ms)) > 1
        assert max(plan.offsets_ms) > 0

    def test_humanized_reduz_o_volume_de_madrugada(self, rng: random.Random) -> None:
        """A mesma campanha planeja MENOS tarefas as 3h que as 19h.

        Comparamos somas de muitos ticks porque a decisao por tick e
        probabilistica -- um unico tick poderia coincidir.
        """

        def total_para(hour: int) -> int:
            r = random.Random(4242)  # mesma semente: isola o efeito da hora
            return sum(
                plan_tick(
                    strategy=JitterStrategy.HUMANIZED,
                    target_rate_per_min=600,
                    tick_seconds=1.0,
                    hour_utc=hour,
                    remaining_budget=100_000,
                    max_batch=200,
                    rng=r,
                ).count
                for _ in range(500)
            )

        assert total_para(3) < total_para(19)
