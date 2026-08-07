"""Testes da maquina de estados do circuit breaker.

O foco esta nas transicoes e, principalmente, nos casos que implementacoes
ingenuas erram:

- sucesso ZERA o contador de falhas (o gatilho e "N falhas CONSECUTIVAS");
- falha em half_open reabre imediatamente e REINICIA o cooldown;
- falha atrasada com o circuito ja aberto NAO reinicia o cooldown -- se
  reiniciasse, o circuito poderia ficar aberto para sempre.
"""

from __future__ import annotations

from apt.domain.models import BreakerState
from apt.resilience.breaker_state import (
    BreakerConfig,
    BreakerSnapshot,
    evaluate_allow,
    evaluate_failure,
    evaluate_success,
)

CONFIG = BreakerConfig(
    failure_threshold=3,
    open_seconds=10,
    half_open_probes=2,
    success_threshold=2,
)


class TestClosed:
    """Comportamento no estado normal."""

    def test_permite_trafego(self, now_ms: int) -> None:
        decision = evaluate_allow(BreakerSnapshot(), config=CONFIG, now_ms=now_ms)
        assert decision.allowed is True
        assert decision.snapshot.state is BreakerState.CLOSED

    def test_falhas_consecutivas_abrem_o_circuito(self, now_ms: int) -> None:
        snapshot = BreakerSnapshot()
        for _ in range(CONFIG.failure_threshold - 1):
            snapshot, transition = evaluate_failure(snapshot, config=CONFIG, now_ms=now_ms)
            assert snapshot.state is BreakerState.CLOSED
            assert transition is None

        snapshot, transition = evaluate_failure(snapshot, config=CONFIG, now_ms=now_ms)
        assert snapshot.state is BreakerState.OPEN
        assert transition == (BreakerState.CLOSED, BreakerState.OPEN)
        assert snapshot.opened_at_ms == now_ms

    def test_sucesso_zera_o_contador_de_falhas(self, now_ms: int) -> None:
        """Duas falhas, um sucesso, duas falhas: o circuito NAO abre.

        Esta e a diferenca entre "3 falhas consecutivas" e "3 falhas no total".
        Falhas isoladas e espacadas fazem parte da vida de qualquer chamada de
        rede -- abrir o circuito por causa delas tornaria o sistema
        desnecessariamente fragil.
        """
        snapshot = BreakerSnapshot()
        snapshot, _ = evaluate_failure(snapshot, config=CONFIG, now_ms=now_ms)
        snapshot, _ = evaluate_failure(snapshot, config=CONFIG, now_ms=now_ms)
        assert snapshot.failure_count == 2

        snapshot, _ = evaluate_success(snapshot, config=CONFIG)
        assert snapshot.failure_count == 0

        snapshot, _ = evaluate_failure(snapshot, config=CONFIG, now_ms=now_ms)
        snapshot, transition = evaluate_failure(snapshot, config=CONFIG, now_ms=now_ms)
        assert snapshot.state is BreakerState.CLOSED
        assert transition is None


class TestOpen:
    """Comportamento com o circuito aberto."""

    def test_nega_durante_o_cooldown(self, now_ms: int) -> None:
        snapshot = BreakerSnapshot(state=BreakerState.OPEN, opened_at_ms=now_ms)
        decision = evaluate_allow(snapshot, config=CONFIG, now_ms=now_ms + 5_000)
        assert decision.allowed is False
        assert decision.retry_after_ms == 5_000

    def test_transita_para_half_open_apos_o_cooldown(self, now_ms: int) -> None:
        snapshot = BreakerSnapshot(state=BreakerState.OPEN, opened_at_ms=now_ms)
        decision = evaluate_allow(snapshot, config=CONFIG, now_ms=now_ms + 10_001)
        assert decision.allowed is True
        assert decision.snapshot.state is BreakerState.HALF_OPEN
        assert decision.transition == (BreakerState.OPEN, BreakerState.HALF_OPEN)
        # A primeira sonda ja foi consumida por esta decisao.
        assert decision.snapshot.probes_in_flight == 1

    def test_falha_atrasada_nao_reinicia_o_cooldown(self, now_ms: int) -> None:
        """Resposta de um envio que estava em voo quando o circuito abriu.

        Se cada resposta atrasada reiniciasse o cooldown, um sistema com muitas
        requisicoes em voo no momento da abertura poderia manter o circuito
        aberto indefinidamente -- mesmo depois da plataforma ter voltado. O
        circuito nunca sondaria a recuperacao.
        """
        opened_at = now_ms
        snapshot = BreakerSnapshot(state=BreakerState.OPEN, opened_at_ms=opened_at)
        snapshot, transition = evaluate_failure(snapshot, config=CONFIG, now_ms=now_ms + 3_000)
        assert snapshot.opened_at_ms == opened_at
        assert transition is None

    def test_sucesso_atrasado_nao_fecha_o_circuito(self, now_ms: int) -> None:
        """Sucesso de envio anterior a abertura nao e sinal de recuperacao.

        A resposta e mais ANTIGA que a decisao de abrir. Trata-la como evidencia
        de que a plataforma voltou fecharia o circuito com base em informacao
        obsoleta.
        """
        snapshot = BreakerSnapshot(state=BreakerState.OPEN, opened_at_ms=now_ms)
        result, transition = evaluate_success(snapshot, config=CONFIG)
        assert result.state is BreakerState.OPEN
        assert transition is None


class TestHalfOpen:
    """Comportamento durante a sondagem de recuperacao."""

    def test_limita_as_sondas_simultaneas(self, now_ms: int) -> None:
        """Alem da cota, os envios sao negados.

        Liberar todo o trafego acumulado de uma vez seria uma rajada sobre um
        servico que acabou de se recuperar -- e o derrubaria outra vez. E o
        motivo de a cota existir.
        """
        snapshot = BreakerSnapshot(
            state=BreakerState.HALF_OPEN, opened_at_ms=now_ms, probes_in_flight=0
        )

        first = evaluate_allow(snapshot, config=CONFIG, now_ms=now_ms)
        assert first.allowed is True
        second = evaluate_allow(first.snapshot, config=CONFIG, now_ms=now_ms)
        assert second.allowed is True
        # A cota (half_open_probes=2) esgotou.
        third = evaluate_allow(second.snapshot, config=CONFIG, now_ms=now_ms)
        assert third.allowed is False

    def test_sucessos_suficientes_fecham_o_circuito(self, now_ms: int) -> None:
        snapshot = BreakerSnapshot(
            state=BreakerState.HALF_OPEN, opened_at_ms=now_ms, probes_in_flight=2
        )
        snapshot, transition = evaluate_success(snapshot, config=CONFIG)
        assert snapshot.state is BreakerState.HALF_OPEN
        assert transition is None

        snapshot, transition = evaluate_success(snapshot, config=CONFIG)
        assert snapshot.state is BreakerState.CLOSED
        assert transition == (BreakerState.HALF_OPEN, BreakerState.CLOSED)
        assert snapshot.failure_count == 0

    def test_qualquer_falha_reabre_e_reinicia_o_cooldown(self, now_ms: int) -> None:
        """Uma unica falha na sondagem reabre o circuito.

        Nao ha tolerancia aqui de proposito: a sonda existe para responder
        "voltou?", e uma falha respondeu "nao". O cooldown reinicia a partir de
        agora, dando a plataforma mais tempo.
        """
        snapshot = BreakerSnapshot(
            state=BreakerState.HALF_OPEN,
            opened_at_ms=now_ms,
            success_count=1,
            probes_in_flight=1,
        )
        reopen_at = now_ms + 12_000
        snapshot, transition = evaluate_failure(snapshot, config=CONFIG, now_ms=reopen_at)
        assert snapshot.state is BreakerState.OPEN
        assert transition == (BreakerState.HALF_OPEN, BreakerState.OPEN)
        assert snapshot.opened_at_ms == reopen_at
        assert snapshot.success_count == 0


class TestCicloCompleto:
    """O ciclo que o teste de resiliencia reproduz de ponta a ponta."""

    def test_closed_open_half_open_closed(self, now_ms: int) -> None:
        """Percorre a recuperacao completa depois de uma falha da plataforma."""
        snapshot = BreakerSnapshot()

        # 1. A plataforma comeca a falhar -> o circuito abre.
        for _ in range(CONFIG.failure_threshold):
            snapshot, _ = evaluate_failure(snapshot, config=CONFIG, now_ms=now_ms)
        assert snapshot.state is BreakerState.OPEN

        # 2. Durante o cooldown, nada passa.
        assert evaluate_allow(snapshot, config=CONFIG, now_ms=now_ms + 1_000).allowed is False

        # 3. Cumprido o cooldown, a primeira sonda passa.
        probe = evaluate_allow(snapshot, config=CONFIG, now_ms=now_ms + 10_500)
        assert probe.allowed is True
        snapshot = probe.snapshot
        assert snapshot.state is BreakerState.HALF_OPEN

        # 4. As sondas tem sucesso -> o circuito fecha.
        for _ in range(CONFIG.success_threshold):
            snapshot, _ = evaluate_success(snapshot, config=CONFIG)
        assert snapshot.state is BreakerState.CLOSED

        # 5. Trafego normal restabelecido.
        assert evaluate_allow(snapshot, config=CONFIG, now_ms=now_ms + 11_000).allowed is True
