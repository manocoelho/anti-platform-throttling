"""Testes do token bucket -- a implementacao de referencia do rate limiter.

Este e o arquivo de teste mais importante do projeto. O token bucket e o
mecanismo central da POC, e como a versao que roda em producao e um script Lua
(que exige Redis), a garantia de correcao do ALGORITMO vem daqui.

A cobertura foca nos casos de borda que quebram implementacoes ingenuas:
bucket vazio, refill fracionario, relogio para tras, pedido maior que a
capacidade, e a garantia de que negar nao consome credito.
"""

from __future__ import annotations

import pytest

from apt.resilience.token_bucket import BucketState, consume, refill


class TestRefill:
    """Reposicao de fichas em funcao do tempo."""

    def test_bucket_cheio_nao_passa_da_capacidade(self, now_ms: int) -> None:
        """Fichas nunca excedem a capacidade, por muito tempo que passe.

        Sem o limite, um bucket parado por uma hora acumularia 57.600 fichas a
        16/s e liberaria uma rajada gigantesca no primeiro envio -- exatamente o
        comportamento que a POC existe para evitar.
        """
        state = BucketState(tokens=16.0, updated_at_ms=now_ms - 3_600_000)
        assert refill(state, capacity=16.0, refill_rps=16.0, now_ms=now_ms) == 16.0

    def test_refill_proporcional_ao_tempo(self, now_ms: int) -> None:
        """Meio segundo a 16 fichas/s reposta 8 fichas."""
        state = BucketState(tokens=0.0, updated_at_ms=now_ms - 500)
        assert refill(state, capacity=16.0, refill_rps=16.0, now_ms=now_ms) == pytest.approx(8.0)

    def test_refill_fracionario_e_preservado(self, now_ms: int) -> None:
        """Fracoes de ficha nao sao truncadas.

        Com aritmetica inteira, uma vazao de 0.5 req/s truncaria para zero a cada
        leitura e o bucket nunca encheria. E o motivo de `tokens` ser float.
        """
        state = BucketState(tokens=0.0, updated_at_ms=now_ms - 100)
        result = refill(state, capacity=4.0, refill_rps=0.5, now_ms=now_ms)
        assert result == pytest.approx(0.05)

    def test_relogio_para_tras_nao_remove_fichas(self, now_ms: int) -> None:
        """Timestamp futuro nao debita fichas do bucket.

        Cenario real: os workers passam `now_ms` do proprio relogio. Uma correcao
        de NTP ou alguns ms de diferenca entre containers podem produzir um
        `now_ms` ANTERIOR ao `updated_at_ms` gravado por outro worker.

        Sem o clamp em zero, o tempo decorrido negativo REMOVERIA fichas -- o
        limite ficaria intermitentemente mais restritivo que o configurado, e a
        causa seria praticamente impossivel de diagnosticar em producao.
        """
        state = BucketState(tokens=10.0, updated_at_ms=now_ms + 5_000)
        assert refill(state, capacity=16.0, refill_rps=16.0, now_ms=now_ms) == 10.0


class TestConsume:
    """Decisao de permitir ou negar."""

    def test_bucket_novo_comeca_cheio(self, now_ms: int) -> None:
        """`state=None` significa bucket inexistente, e ele nasce CHEIO.

        Nascer vazio faria a primeira requisicao de cada URL nova esperar sem
        motivo, e todo deploy introduziria uma pausa artificial no trafego.
        """
        decision = consume(None, capacity=16.0, refill_rps=16.0, now_ms=now_ms)
        assert decision.allowed is True
        assert decision.tokens_remaining == pytest.approx(15.0)

    def test_consome_uma_ficha_por_padrao(self, now_ms: int) -> None:
        state = BucketState(tokens=5.0, updated_at_ms=now_ms)
        decision = consume(state, capacity=16.0, refill_rps=16.0, now_ms=now_ms)
        assert decision.allowed is True
        assert decision.tokens_remaining == pytest.approx(4.0)

    def test_nega_quando_nao_ha_ficha(self, now_ms: int) -> None:
        state = BucketState(tokens=0.0, updated_at_ms=now_ms)
        decision = consume(state, capacity=16.0, refill_rps=16.0, now_ms=now_ms)
        assert decision.allowed is False
        assert decision.retry_after_ms > 0

    def test_negativa_nao_consome_credito(self, now_ms: int) -> None:
        """Negar mantem as fichas disponiveis intactas.

        Se negar debitasse, um cliente insistente empurraria o saldo para
        negativo e atrasaria indefinidamente os demais -- uma forma acidental de
        negacao de servico interna.
        """
        state = BucketState(tokens=0.4, updated_at_ms=now_ms)
        decision = consume(state, capacity=16.0, refill_rps=16.0, now_ms=now_ms)
        assert decision.allowed is False
        assert decision.state.tokens == pytest.approx(0.4)

    def test_retry_after_permite_a_proxima_tentativa(self, now_ms: int) -> None:
        """Esperar exatamente `retry_after_ms` e suficiente para ser aceito.

        Esta e a propriedade que faz o backoff funcionar. Se o prazo devolvido
        fosse curto por um milissegundo, o cliente voltaria cedo, seria negado de
        novo, e numa fila movimentada isso viraria um ciclo de tentativas inuteis.
        E por isso que o calculo arredonda para cima e soma 1ms.
        """
        state = BucketState(tokens=0.0, updated_at_ms=now_ms)
        first = consume(state, capacity=16.0, refill_rps=16.0, now_ms=now_ms)
        assert first.allowed is False

        later = consume(
            first.state,
            capacity=16.0,
            refill_rps=16.0,
            now_ms=now_ms + first.retry_after_ms,
        )
        assert later.allowed is True

    def test_pedido_maior_que_capacidade_e_negado_sem_prazo(self, now_ms: int) -> None:
        """Pedir mais que a capacidade nega com `retry_after_ms == 0`.

        Devolver um prazo seria mentir: por mais que se espere, o bucket nunca
        tera 20 fichas se a capacidade e 16. Prazo zero sinaliza "nao insista"
        e evita espera perpetua.
        """
        state = BucketState(tokens=16.0, updated_at_ms=now_ms)
        decision = consume(state, capacity=16.0, refill_rps=16.0, now_ms=now_ms, requested=20.0)
        assert decision.allowed is False
        assert decision.retry_after_ms == 0

    def test_requested_zero_permite_inspecao_sem_debito(self, now_ms: int) -> None:
        """`requested=0` faz o refill e devolve o saldo sem consumir.

        E o mecanismo usado por `RateLimiter.peek()` para alimentar a metrica de
        gauge sem alterar o comportamento do sistema.
        """
        state = BucketState(tokens=3.0, updated_at_ms=now_ms - 1000)
        decision = consume(state, capacity=16.0, refill_rps=16.0, now_ms=now_ms, requested=0.0)
        assert decision.allowed is True
        assert decision.tokens_remaining == pytest.approx(16.0)

    @pytest.mark.parametrize(
        ("capacity", "refill_rps"),
        [(0.0, 16.0), (-1.0, 16.0), (16.0, 0.0), (16.0, -5.0)],
    )
    def test_parametros_invalidos_estouram(
        self, capacity: float, refill_rps: float, now_ms: int
    ) -> None:
        """Capacidade ou vazao nao positivas sao erro de configuracao.

        `refill_rps=0` causaria divisao por zero no calculo do prazo, e
        `capacity=0` negaria tudo para sempre. Falhar alto e melhor que operar
        com um limiter silenciosamente quebrado.
        """
        with pytest.raises(ValueError):
            consume(None, capacity=capacity, refill_rps=refill_rps, now_ms=now_ms)


class TestSequenciaRealista:
    """Sequencias que reproduzem o comportamento sob carga."""

    def test_rajada_esgota_bucket_e_depois_converge_para_a_vazao(self, now_ms: int) -> None:
        """A rajada inicial consome a capacidade; depois a vazao passa a mandar.

        E a propriedade central do token bucket, e a razao de te-lo escolhido em
        vez de janela fixa: ele tolera uma rajada CONTROLADA (do tamanho da
        capacidade) e em seguida converge para a vazao sustentada.
        """
        capacity, rps = 16.0, 16.0
        state: BucketState | None = None
        allowed_in_burst = 0

        # Rajada instantanea de 20 requisicoes no mesmo milissegundo.
        for _ in range(20):
            decision = consume(state, capacity=capacity, refill_rps=rps, now_ms=now_ms)
            state = decision.state
            if decision.allowed:
                allowed_in_burst += 1

        # Exatamente a capacidade passa; as 4 restantes sao negadas.
        assert allowed_in_burst == 16

        # Um segundo depois, o bucket recuperou aproximadamente `rps` fichas.
        after = consume(
            state, capacity=capacity, refill_rps=rps, now_ms=now_ms + 1000, requested=0.0
        )
        assert after.tokens_remaining == pytest.approx(16.0)

    def test_vazao_sustentada_respeita_o_limite_configurado(self, now_ms: int) -> None:
        """Ao longo de 10 segundos, o total aceito nao passa de `capacity + rps*10`.

        E a garantia que o teste de escala verifica de ponta a ponta: o limite
        vale independentemente de quantos processos consultam o bucket, porque o
        estado e um so. Aqui verificamos a aritmetica que sustenta essa
        propriedade.
        """
        capacity, rps = 16.0, 16.0
        state: BucketState | None = None
        accepted = 0

        # Uma tentativa a cada 10ms por 10 segundos = 1000 tentativas,
        # equivalente a uma demanda de 100 req/s contra um limite de 16 req/s.
        for step in range(1000):
            decision = consume(state, capacity=capacity, refill_rps=rps, now_ms=now_ms + step * 10)
            state = decision.state
            if decision.allowed:
                accepted += 1

        teto_teorico = capacity + rps * 10  # rajada inicial + 10s de refill
        assert accepted <= teto_teorico
        # E confirma que nao ficou desnecessariamente restritivo: deve chegar
        # perto do teto, nao aceitar so um punhado.
        assert accepted >= teto_teorico * 0.9
