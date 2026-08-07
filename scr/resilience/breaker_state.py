"""Maquina de estados do circuit breaker -- implementacao pura de referencia.

Mesma estrategia do token bucket: a logica vive aqui em funcoes puras e
testaveis, e o que roda em producao e o script Lua equivalente (que garante
atomicidade entre workers). Ver o docstring de `token_bucket.py` para a
justificativa completa da duplicacao.

A MAQUINA DE ESTADOS

    CLOSED ---- failure_count >= failure_threshold ----> OPEN
      ^                                                  |
      |                                          passou open_seconds
      |                                                  |
      |                                                  v
      +---- success_count >= success_threshold ----- HALF_OPEN
                                                         |
                        qualquer falha em half_open ------+--> OPEN

CLOSED
    Operacao normal. Contamos falhas consecutivas. Um sucesso zera o contador --
    o gatilho e "N falhas EM SEQUENCIA", nao "N falhas no total". Falhas
    isoladas e espacadas nao devem abrir o circuito; elas fazem parte da vida de
    qualquer chamada de rede.

OPEN
    Recusamos os envios sem nem tentar chamar a plataforma. Este e o ponto do
    padrao: se a plataforma esta com problema, insistir (a) nao vai funcionar,
    (b) consome recursos nossos em timeouts, (c) piora a situacao dela e (d) no
    caso especifico de throttling, prolonga a punicao -- muitas plataformas
    estendem o bloqueio a cada requisicao recebida durante a penalidade.

HALF_OPEN
    Depois de `open_seconds`, deixamos passar poucas sondas para descobrir se a
    plataforma voltou. Numero limitado de proposito: liberar todo o trafego
    acumulado de uma vez seria uma rajada exatamente sobre um servico que
    acabou de se recuperar -- e o derrubaria outra vez. Uma falha em half_open
    reabre o circuito imediatamente.

O QUE CONTA COMO FALHA

Somente rejeicoes da PLATAFORMA: 429, 5xx e timeout (ver
`Outcome.is_platform_rejection`). Os adiamentos internos -- rate limiter,
bulkhead, circuito aberto -- nao contam.

A distincao e essencial. Se o adiamento do rate limiter contasse como falha, o
rate limiter funcionando corretamente abriria o circuit breaker, e o sistema se
autobloquearia sem que a plataforma tivesse reclamado de nada. Foi o primeiro
bug conceitual que aparece quando se junta os dois padroes.
"""

from __future__ import annotations

from dataclasses import dataclass

from apt.domain.models import BreakerState


@dataclass(frozen=True, slots=True)
class BreakerConfig:
    """Parametros de um circuito.

    Attributes:
        failure_threshold: falhas consecutivas para abrir.
        open_seconds: quanto tempo permanecer aberto antes de admitir sondas.
        half_open_probes: sondas simultaneas permitidas em half_open.
        success_threshold: sucessos consecutivos em half_open para fechar.
    """

    failure_threshold: int = 5
    open_seconds: int = 15
    half_open_probes: int = 2
    success_threshold: int = 3


@dataclass(frozen=True, slots=True)
class BreakerSnapshot:
    """Estado completo de um circuito num instante.

    Attributes:
        state: estado atual.
        failure_count: falhas consecutivas (relevante em CLOSED).
        success_count: sucessos consecutivos (relevante em HALF_OPEN).
        opened_at_ms: quando o circuito abriu pela ultima vez. Zero se nunca.
        probes_in_flight: sondas em andamento (relevante em HALF_OPEN).
    """

    state: BreakerState = BreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    opened_at_ms: int = 0
    probes_in_flight: int = 0


@dataclass(frozen=True, slots=True)
class AllowDecision:
    """Resposta a pergunta "posso enviar agora?".

    Attributes:
        allowed: se o envio pode seguir.
        snapshot: o estado apos a decisao (pedir uma sonda ALTERA o estado --
            incrementa `probes_in_flight` -- por isso a decisao devolve o novo
            estado, e nao apenas um booleano).
        transition: `(de, para)` se houve mudanca de estado, senao `None`.
            Usado para registrar o evento em `breaker_events` e emitir log.
        retry_after_ms: quando negado, quanto falta para a proxima janela de
            sondagem.
    """

    allowed: bool
    snapshot: BreakerSnapshot
    transition: tuple[BreakerState, BreakerState] | None = None
    retry_after_ms: int = 0


def evaluate_allow(
    snapshot: BreakerSnapshot, *, config: BreakerConfig, now_ms: int
) -> AllowDecision:
    """Decide se um envio pode passar, dado o estado atual.

    Funcao pura: nao toca em Redis nem em relogio. `now_ms` e injetado, o que
    torna possivel testar a transicao OPEN -> HALF_OPEN sem esperar segundos de
    verdade.
    """
    if snapshot.state is BreakerState.CLOSED:
        return AllowDecision(allowed=True, snapshot=snapshot)

    if snapshot.state is BreakerState.OPEN:
        elapsed_ms = now_ms - snapshot.opened_at_ms
        cooldown_ms = config.open_seconds * 1000

        if elapsed_ms < cooldown_ms:
            return AllowDecision(
                allowed=False,
                snapshot=snapshot,
                retry_after_ms=max(0, cooldown_ms - elapsed_ms),
            )

        # Cooldown cumprido: entra em half_open e consome a primeira sonda.
        # Zeramos `success_count` para que a contagem de sucessos da janela de
        # recuperacao comece limpa -- sucessos de antes da falha nao devem
        # contar para fechar o circuito agora.
        return AllowDecision(
            allowed=True,
            snapshot=BreakerSnapshot(
                state=BreakerState.HALF_OPEN,
                failure_count=snapshot.failure_count,
                success_count=0,
                opened_at_ms=snapshot.opened_at_ms,
                probes_in_flight=1,
            ),
            transition=(BreakerState.OPEN, BreakerState.HALF_OPEN),
        )

    # HALF_OPEN: admite sondas ate o limite configurado.
    if snapshot.probes_in_flight < config.half_open_probes:
        return AllowDecision(
            allowed=True,
            snapshot=BreakerSnapshot(
                state=BreakerState.HALF_OPEN,
                failure_count=snapshot.failure_count,
                success_count=snapshot.success_count,
                opened_at_ms=snapshot.opened_at_ms,
                probes_in_flight=snapshot.probes_in_flight + 1,
            ),
        )

    # Cota de sondas esgotada: os demais envios esperam o resultado das que
    # estao em voo. O prazo sugerido e curto porque a resposta deve chegar logo.
    return AllowDecision(
        allowed=False,
        snapshot=snapshot,
        retry_after_ms=1000,
    )


def evaluate_success(
    snapshot: BreakerSnapshot, *, config: BreakerConfig
) -> tuple[BreakerSnapshot, tuple[BreakerState, BreakerState] | None]:
    """Aplica um sucesso ao estado. Devolve `(novo_estado, transicao)`."""
    if snapshot.state is BreakerState.CLOSED:
        # Sucesso zera as falhas consecutivas. E o que faz o gatilho ser "N
        # falhas em sequencia" e nao "N falhas somadas ao longo do tempo".
        if snapshot.failure_count == 0:
            return snapshot, None
        return (
            BreakerSnapshot(
                state=BreakerState.CLOSED,
                failure_count=0,
                success_count=0,
                opened_at_ms=snapshot.opened_at_ms,
                probes_in_flight=0,
            ),
            None,
        )

    if snapshot.state is BreakerState.HALF_OPEN:
        successes = snapshot.success_count + 1
        # A sonda terminou: libera o slot.
        probes = max(0, snapshot.probes_in_flight - 1)

        if successes >= config.success_threshold:
            return (
                BreakerSnapshot(state=BreakerState.CLOSED),
                (BreakerState.HALF_OPEN, BreakerState.CLOSED),
            )

        return (
            BreakerSnapshot(
                state=BreakerState.HALF_OPEN,
                failure_count=snapshot.failure_count,
                success_count=successes,
                opened_at_ms=snapshot.opened_at_ms,
                probes_in_flight=probes,
            ),
            None,
        )

    # Sucesso registrado com o circuito OPEN: acontece quando um envio ja estava
    # em voo no instante em que o circuito abriu. Nao mexemos no estado -- a
    # resposta e mais antiga que a decisao de abrir, e trata-la como evidencia
    # de recuperacao fecharia o circuito com base em informacao obsoleta.
    return snapshot, None


def evaluate_failure(
    snapshot: BreakerSnapshot, *, config: BreakerConfig, now_ms: int
) -> tuple[BreakerSnapshot, tuple[BreakerState, BreakerState] | None]:
    """Aplica uma falha ao estado. Devolve `(novo_estado, transicao)`."""
    if snapshot.state is BreakerState.HALF_OPEN:
        # Qualquer falha durante a sondagem reabre imediatamente. Nao ha
        # tolerancia aqui de proposito: a sonda existe para responder "voltou?",
        # e uma falha respondeu "nao". Reiniciamos o cooldown a partir de agora.
        return (
            BreakerSnapshot(
                state=BreakerState.OPEN,
                failure_count=config.failure_threshold,
                success_count=0,
                opened_at_ms=now_ms,
                probes_in_flight=0,
            ),
            (BreakerState.HALF_OPEN, BreakerState.OPEN),
        )

    if snapshot.state is BreakerState.OPEN:
        # Falha de um envio que ja estava em voo. Nao reinicia o cooldown --
        # reiniciar a cada resposta atrasada poderia manter o circuito aberto
        # indefinidamente mesmo depois da plataforma ter voltado.
        return snapshot, None

    # CLOSED
    failures = snapshot.failure_count + 1
    if failures >= config.failure_threshold:
        return (
            BreakerSnapshot(
                state=BreakerState.OPEN,
                failure_count=failures,
                success_count=0,
                opened_at_ms=now_ms,
                probes_in_flight=0,
            ),
            (BreakerState.CLOSED, BreakerState.OPEN),
        )

    return (
        BreakerSnapshot(
            state=BreakerState.CLOSED,
            failure_count=failures,
            success_count=0,
            opened_at_ms=snapshot.opened_at_ms,
            probes_in_flight=0,
        ),
        None,
    )
