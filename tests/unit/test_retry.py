"""Testes da politica de retry: backoff com jitter e escolha do degrau.

O ponto mais importante aqui e a verificacao estatistica do FULL JITTER. Um
backoff exponencial puro produz atrasos identicos para todos os clientes que
falharam junto -- o "thundering herd". O jitter existe para quebrar essa
sincronia, e a propriedade que comprova isso e a DISPERSAO dos valores, nao a
media.
"""

from __future__ import annotations

import statistics

import pytest

from apt.messaging.topology import RETRY_TIERS_MS
from apt.resilience.retry import (
    backoff_ms,
    choose_tier,
    is_retryable_status,
    tier_for_attempt,
    tier_for_retry_after,
)


class TestBackoff:
    def test_nunca_devolve_zero(self) -> None:
        """Atraso zero faria a proxima tentativa sair no mesmo instante.

        O full jitter sorteia no intervalo [0, teto], entao o zero e um resultado
        possivel do sorteio -- e por isso o codigo forca o minimo de 1ms.
        """
        assert all(backoff_ms(attempt) >= 1 for attempt in range(1, 10) for _ in range(50))

    def test_teto_cresce_exponencialmente(self) -> None:
        """O MAXIMO observado cresce com a tentativa.

        Comparamos maximos de varias amostras, e nao valores unicos: com full
        jitter, um sorteio da tentativa 4 pode perfeitamente ser menor que um da
        tentativa 1. E a media que sobe, e o teto que a limita.
        """
        max_attempt_1 = max(backoff_ms(1) for _ in range(500))
        max_attempt_4 = max(backoff_ms(4) for _ in range(500))
        assert max_attempt_4 > max_attempt_1

    def test_respeita_o_teto_configurado(self) -> None:
        """Com tentativa alta, o atraso nao passa do `max_ms`.

        Sem o `min` antes do sorteio, `base * 2**attempt` com attempt=40 geraria
        um inteiro absurdo -- e uma tarefa esperaria anos.
        """
        assert all(backoff_ms(40, base_ms=500, max_ms=10_000) <= 10_000 for _ in range(200))

    def test_full_jitter_produz_dispersao_alta(self) -> None:
        """A dispersao e a propriedade que evita o thundering herd.

        Verificamos que o desvio-padrao e uma fracao substancial da media. Num
        backoff SEM jitter, todas as amostras seriam identicas e o desvio seria
        zero -- o que significaria que todos os clientes voltam no mesmo
        instante, exatamente o problema que o jitter resolve.
        """
        amostras = [backoff_ms(3, base_ms=1000, max_ms=60_000) for _ in range(1000)]
        media = statistics.mean(amostras)
        desvio = statistics.stdev(amostras)
        assert media > 0
        # Para uma uniforme em [0, T], o desvio e ~28.9% da media.
        assert desvio / media > 0.2

    def test_attempt_zero_ou_negativo_e_normalizado(self) -> None:
        """Valor invalido nao estoura -- e tratado como primeira tentativa.

        `attempt` chega de dentro do payload de uma mensagem, que pode ter sido
        publicada por outra versao do sistema. Ser tolerante aqui evita mandar
        para a DLQ uma tarefa perfeitamente valida.
        """
        assert backoff_ms(0) >= 1
        assert backoff_ms(-5) >= 1


class TestChooseTier:
    def test_arredonda_para_cima(self) -> None:
        """Escolhe o primeiro degrau MAIOR OU IGUAL ao atraso desejado.

        Arredondar para baixo faria a tarefa voltar antes da plataforma ter se
        recuperado; esperar um pouco mais e inofensivo.
        """
        assert choose_tier(1) == 1  # <= 1000ms  -> degrau 1
        assert choose_tier(1_000) == 1
        assert choose_tier(1_001) == 2  # <= 5000ms  -> degrau 2
        assert choose_tier(5_000) == 2
        assert choose_tier(5_001) == 3  # <= 30000ms -> degrau 3
        assert choose_tier(30_000) == 3

    def test_atraso_acima_do_ultimo_degrau_usa_o_ultimo(self) -> None:
        """Nao existe degrau maior; o ultimo e o teto disponivel."""
        assert choose_tier(10_000_000) == len(RETRY_TIERS_MS)

    def test_sempre_devolve_degrau_valido(self) -> None:
        for delay in (0, 1, 999, 1000, 4999, 30_001, 999_999):
            tier = choose_tier(delay)
            assert 1 <= tier <= len(RETRY_TIERS_MS)


class TestTierHelpers:
    def test_tier_for_attempt_devolve_degrau_e_atraso(self) -> None:
        tier, delay = tier_for_attempt(2)
        assert 1 <= tier <= len(RETRY_TIERS_MS)
        assert delay >= 1

    def test_tier_for_retry_after_respeita_o_prazo_pedido(self) -> None:
        """O degrau escolhido cobre o `Retry-After` informado pela plataforma.

        Sem jitter aqui de proposito: quando a plataforma diz "espere 30s", e
        instrucao, nao estimativa. Sortear um numero menor seria desobedecer -- e
        ignorar um `Retry-After` explicito e a forma mais rapida de escalar de
        throttling para bloqueio.
        """
        tier = tier_for_retry_after(5_000)
        assert RETRY_TIERS_MS[tier - 1] >= 5_000

        tier = tier_for_retry_after(30_000)
        assert RETRY_TIERS_MS[tier - 1] >= 30_000

    def test_retry_after_zero_ou_negativo_usa_o_primeiro_degrau(self) -> None:
        assert tier_for_retry_after(0) == 1
        assert tier_for_retry_after(-100) == 1


class TestIsRetryableStatus:
    @pytest.mark.parametrize("status", [429, 408, 500, 502, 503, 504])
    def test_retentaveis(self, status: int) -> None:
        assert is_retryable_status(status) is True

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
    def test_nao_retentaveis(self, status: int) -> None:
        """4xx (exceto 408/429) nao mudam de resultado ao serem repetidos.

        Retentar um 404 gasta cota do rate limiter e atrasa tarefas que teriam
        sucesso. Vao direto para a DLQ, onde alguem pode olhar.
        """
        assert is_retryable_status(status) is False

    @pytest.mark.parametrize("status", [200, 201, 204, 301, 302])
    def test_sucesso_e_redirect_nao_sao_retentaveis(self, status: int) -> None:
        assert is_retryable_status(status) is False
