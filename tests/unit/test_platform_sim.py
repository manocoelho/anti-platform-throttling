"""Testes do simulador de plataformas.

O simulador e o instrumento de medicao da POC -- se ele estiver errado, todos os
resultados dos testes de carga estao errados. Por isso a janela deslizante e o
`peak_rps` sao verificados aqui, com o relogio controlado por monkeypatch em vez
de `sleep` real (um teste que dorme 1 segundo por caso tornaria a suite lenta sem
ganho de confianca).
"""

from __future__ import annotations

import pytest

from apt.platform_sim.throttle import FaultConfig, FaultMode, PlatformThrottle


class TestJanelaDeslizante:
    def test_aceita_ate_o_limite(self) -> None:
        throttle = PlatformThrottle(limit_rps=5)
        for _ in range(5):
            accepted, _ = throttle.try_accept()
            assert accepted is True
        assert throttle.total_accepted == 5

    def test_recusa_acima_do_limite(self) -> None:
        throttle = PlatformThrottle(limit_rps=3)
        for _ in range(3):
            throttle.try_accept()

        accepted, retry_after = throttle.try_accept()
        assert accepted is False
        assert retry_after >= 1
        assert throttle.total_throttled == 1

    def test_janela_desliza_com_o_tempo(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Passado 1 segundo, as requisicoes antigas saem da janela.

        Controlamos o relogio por monkeypatch em vez de dormir de verdade: o
        comportamento verificado e a expiracao da janela, e nao a passagem real do
        tempo.
        """
        clock = {"now": 1000.0}
        monkeypatch.setattr("apt.platform_sim.throttle.time.monotonic", lambda: clock["now"])

        throttle = PlatformThrottle(limit_rps=2)
        assert throttle.try_accept()[0] is True
        assert throttle.try_accept()[0] is True
        assert throttle.try_accept()[0] is False

        # Avanca 1.1s: a janela expira e volta a aceitar.
        clock["now"] += 1.1
        assert throttle.try_accept()[0] is True

    def test_peak_rps_registra_o_maximo_observado(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """`peak_rps` e o numero central do relatorio de testes.

        E o pico que a PLATAFORMA realmente observou. Se ficou abaixo do limite
        dela durante todo o teste, o rate limiter do cliente cumpriu o objetivo --
        e essa e a evidencia medida do lado de quem imporia a punicao.
        """
        clock = {"now": 1000.0}
        monkeypatch.setattr("apt.platform_sim.throttle.time.monotonic", lambda: clock["now"])

        throttle = PlatformThrottle(limit_rps=10)
        for _ in range(4):
            throttle.try_accept()
        assert throttle.peak_rps == 4

        # Nova janela com menos requisicoes: o pico historico NAO diminui.
        clock["now"] += 2.0
        throttle.try_accept()
        assert throttle.peak_rps == 4

    def test_reset_limpa_tudo(self) -> None:
        throttle = PlatformThrottle(limit_rps=3)
        for _ in range(5):
            throttle.try_accept()

        throttle.reset()
        assert throttle.total_accepted == 0
        assert throttle.total_throttled == 0
        assert throttle.peak_rps == 0
        assert throttle.current_rps() == 0

    def test_current_rps_nao_registra_requisicao(self) -> None:
        """Consultar a contagem nao pode alterar a contagem."""
        throttle = PlatformThrottle(limit_rps=5)
        throttle.try_accept()
        antes = throttle.total_accepted
        assert throttle.current_rps() == 1
        assert throttle.total_accepted == antes


class TestFaultConfig:
    def test_none_nao_esta_ativa(self) -> None:
        assert FaultConfig().active is False

    def test_falha_sem_ttl_e_permanente(self) -> None:
        assert FaultConfig(mode=FaultMode.ERROR_500).active is True

    def test_falha_com_ttl_expira(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A auto-expiracao permite observar o ciclo completo do breaker.

        Sem ela, o teste de resiliencia precisaria de uma chamada externa no meio
        da medicao para remover a falha -- e o momento exato dessa chamada
        influenciaria o resultado.
        """
        clock = {"now": 500.0}
        monkeypatch.setattr("apt.platform_sim.throttle.time.monotonic", lambda: clock["now"])

        fault = FaultConfig(mode=FaultMode.ERROR_500, expires_at=clock["now"] + 10.0)
        assert fault.active is True

        clock["now"] += 10.1
        assert fault.active is False

    @pytest.mark.parametrize(
        "mode",
        [FaultMode.ERROR_500, FaultMode.TIMEOUT, FaultMode.THROTTLE_HARD],
    )
    def test_todos_os_modos_ficam_ativos(self, mode: FaultMode) -> None:
        assert FaultConfig(mode=mode).active is True
