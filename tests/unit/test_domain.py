"""Testes do dominio: contrato da mensagem, classificacao de resultados e perfis.

O teste mais importante deste arquivo e `test_defers_nao_incrementa_attempt`. Ele
guarda a correcao de um bug conceitual real do projeto: com um contador unico,
uma tarefa adiada pelo rate limiter quatro vezes ia para a DLQ sem NUNCA ter sido
enviada -- o sistema descartava trabalho legitimo justamente quando estava se
protegendo corretamente.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from apt.domain.models import (
    ControlMessage,
    Outcome,
    Platform,
    SendTaskMessage,
    utcnow,
)
from apt.domain.platforms import PLATFORM_PROFILES, all_platforms, get_profile


def build_message(**overrides: object) -> SendTaskMessage:
    base: dict[str, object] = {
        "task_id": "11111111-1111-1111-1111-111111111111",
        "campaign_id": "22222222-2222-2222-2222-222222222222",
        "content_id": "33333333-3333-3333-3333-333333333333",
        "platform": Platform.YOUTUBE,
        "content_url": "https://youtube.com/watch?v=teste",
        "correlation_id": "abc123",
        "scheduled_at": "2026-08-07T12:00:00+00:00",
    }
    base.update(overrides)
    return SendTaskMessage(**base)  # type: ignore[arg-type]


class TestSendTaskMessage:
    def test_round_trip_preserva_os_campos(self) -> None:
        original = build_message(attempt=2, defers=7)
        restored = SendTaskMessage.from_dict(original.to_dict())
        assert restored == original

    def test_campos_ausentes_usam_default(self) -> None:
        """Mensagem sem `attempt`/`defers` e aceita.

        Durante um deploy, mensagens publicadas pela versao anterior podem estar
        na fila. Estourar aqui mandaria todas elas para a DLQ.
        """
        payload = build_message().to_dict()
        del payload["attempt"]
        del payload["defers"]
        restored = SendTaskMessage.from_dict(payload)
        assert restored.attempt == 0
        assert restored.defers == 0

    def test_with_attempt_nao_mexe_nos_defers(self) -> None:
        message = build_message(attempt=1, defers=5)
        updated = message.with_attempt(2)
        assert updated.attempt == 2
        assert updated.defers == 5

    def test_defers_nao_incrementa_attempt(self) -> None:
        """ADIAMENTO NAO CONSOME TENTATIVA -- a distincao central do contrato.

        Num sistema saudavel sob carga, os adiamentos sao frequentes e esperados:
        e o rate limiter fazendo o seu trabalho. Se incrementassem `attempt`, uma
        tarefa adiada `max_attempts` vezes iria para a DLQ sem nunca ter sido
        enviada.
        """
        message = build_message(attempt=0, defers=0)
        for _ in range(50):
            message = message.with_defer()
        assert message.defers == 50
        assert message.attempt == 0

    def test_mensagem_e_imutavel(self) -> None:
        """`frozen=True`: um worker nunca altera a mensagem que recebeu.

        A unica mutacao legitima e produzir uma COPIA (`with_attempt`,
        `with_defer`) ao reenfileirar.
        """
        message = build_message()
        with pytest.raises(FrozenInstanceError):
            message.attempt = 5  # type: ignore[misc]


class TestControlMessage:
    def test_round_trip(self) -> None:
        original = ControlMessage(type="flags_changed", payload={"flag": "x", "value": False})
        assert ControlMessage.from_dict(original.to_dict()) == original

    def test_payload_ausente_vira_dicionario_vazio(self) -> None:
        assert ControlMessage.from_dict({"type": "flags_changed"}).payload == {}

    def test_payload_de_tipo_errado_e_ignorado(self) -> None:
        """Payload que nao e dicionario nao derruba o consumidor de controle."""
        assert ControlMessage.from_dict({"type": "x", "payload": "invalido"}).payload == {}


class TestOutcome:
    def test_apenas_sent_e_sucesso(self) -> None:
        assert Outcome.SENT.is_success is True
        for outcome in Outcome:
            if outcome is not Outcome.SENT:
                assert outcome.is_success is False

    def test_rejeicao_da_plataforma(self) -> None:
        """Somente estes tres resultados alimentam o circuit breaker."""
        assert Outcome.THROTTLED.is_platform_rejection is True
        assert Outcome.ERROR.is_platform_rejection is True
        assert Outcome.TIMEOUT.is_platform_rejection is True

    def test_autolimitacao_nao_conta_como_rejeicao(self) -> None:
        """ADIAMENTOS NOSSOS NAO ABREM O CIRCUITO.

        Se o adiamento do rate limiter contasse como falha da plataforma, o
        proprio rate limiter abriria o circuit breaker ao fazer o seu trabalho --
        e o sistema se autobloquearia sem que a plataforma tivesse reclamado de
        nada. E o primeiro bug conceitual que aparece ao juntar os dois padroes.
        """
        for outcome in (
            Outcome.RATE_LIMITED_LOCAL,
            Outcome.CIRCUIT_OPEN,
            Outcome.BULKHEAD_FULL,
        ):
            assert outcome.is_self_throttled is True
            assert outcome.is_platform_rejection is False

    def test_os_dois_grupos_sao_disjuntos(self) -> None:
        """Nenhum resultado e simultaneamente rejeicao externa e adiamento interno."""
        for outcome in Outcome:
            assert not (outcome.is_platform_rejection and outcome.is_self_throttled)


class TestPlatformProfiles:
    def test_toda_plataforma_do_enum_tem_perfil(self) -> None:
        """Um perfil ausente significaria enviar sem limite conhecido.

        E justamente o que a POC existe para evitar, entao a cobertura do enum e
        uma invariante do sistema.
        """
        assert set(PLATFORM_PROFILES) == set(Platform)

    def test_allowed_rps_fica_abaixo_do_limite_estimado(self) -> None:
        """A margem de seguranca e uma invariante, nao uma preferencia.

        Sem folga, o desalinhamento entre a nossa janela de contagem e a da
        plataforma ja seria suficiente para estourar o limite dela -- e um retry
        legitimo tambem.
        """
        for profile in PLATFORM_PROFILES.values():
            assert profile.allowed_rps < profile.estimated_limit_rps

    def test_margem_de_seguranca_de_pelo_menos_10_por_cento(self) -> None:
        for profile in PLATFORM_PROFILES.values():
            assert profile.safety_margin >= 0.10

    def test_burst_nao_passa_do_limite_estimado(self) -> None:
        """Uma rajada nao pode, sozinha, estourar o limite da plataforma.

        Se `burst_capacity` fosse maior que `estimated_limit_rps`, o bucket cheio
        liberaria de uma vez mais requisicoes do que a plataforma aceita num
        segundo -- e receberiamos 429 no primeiro envio apos um periodo de
        inatividade.
        """
        for profile in PLATFORM_PROFILES.values():
            assert profile.burst_capacity <= profile.estimated_limit_rps

    def test_plataformas_tem_limites_assimetricos(self) -> None:
        """A assimetria e proposital: e o que torna o bulkhead observavel.

        Com limites iguais, as duas plataformas degradariam juntas e nao daria
        para demonstrar isolamento.
        """
        limites = {p.allowed_rps for p in PLATFORM_PROFILES.values()}
        assert len(limites) == len(PLATFORM_PROFILES)

    def test_all_platforms_bate_com_o_dicionario(self) -> None:
        assert set(all_platforms()) == set(PLATFORM_PROFILES)

    def test_get_profile_de_plataforma_conhecida(self) -> None:
        profile = get_profile(Platform.YOUTUBE)
        assert profile.platform is Platform.YOUTUBE
        assert profile.endpoint_path.startswith("/youtube")


class TestUtcnow:
    def test_tem_timezone(self) -> None:
        """`utcnow()` devolve datetime com tzinfo.

        Existe para que ninguem use `datetime.utcnow()`, que devolve datetime
        ingenuo e produz comparacoes silenciosamente erradas contra as colunas
        `TIMESTAMPTZ` do Postgres.
        """
        assert utcnow().tzinfo is not None
