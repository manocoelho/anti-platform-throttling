"""Worker: consome a fila e aplica as politicas antes de cada envio.

E aqui que os seis padroes se encontram. A ordem em que sao aplicados NAO e
arbitraria -- cada camada e mais barata que a seguinte, e recusar cedo evita
gastar o recurso da proxima:

    1. FEATURE FLAGS     leitura de cache local, custo ~zero
    2. BULKHEAD          semaforo em memoria; sem slot, nada mais faz sentido
    3. CIRCUIT BREAKER   1 ida ao Redis; se a plataforma esta fora, nao gaste ficha
    4. RATE LIMITER      2 idas ao Redis (conteudo + plataforma)
    5. ENVIO             a chamada de rede, a operacao mais cara

Por que o breaker vem ANTES do rate limiter: consumir uma ficha do balde para
depois descobrir que o circuito esta aberto desperdicaria cota -- e a ficha nao
volta. Invertendo, o circuito filtra primeiro e o balde so e tocado quando o
envio tem chance real de acontecer.

O DESTINO DE CADA MENSAGEM

    ack               sucesso, ou falha definitiva (nao retentavel)
    republish + ack   adiamento (defer) ou retry com backoff
    dead + ack        esgotou tentativas -> DLQ + tabela `failures`

Sempre publicamos ANTES de dar ack. Se o processo morrer entre os dois, o broker
reentrega a mensagem original e a tarefa e processada duas vezes -- duplicidade.
Na ordem inversa (ack primeiro), uma falha na publicacao perderia a tarefa para
sempre. Escolhemos at-least-once: duplicar e recuperavel, perder nao e. A
alternativa completa seria idempotencia ponta a ponta, que ficou fora do escopo
(docs/TRADE-OFFS.md).
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
from typing import Any
from uuid import UUID, uuid4

from aio_pika.abc import AbstractIncomingMessage
from prometheus_client import start_http_server

from apt import __version__
from apt.config import get_settings
from apt.db.engine import connection, dispose_engine
from apt.db.repositories import (
    BreakerEventRepository,
    CampaignRepository,
    ExecutionRepository,
    FailureRepository,
    TaskRepository,
)
from apt.domain.models import (
    BreakerState,
    ControlMessage,
    ExecutionRecord,
    Outcome,
    Platform,
    SendTaskMessage,
    TaskStatus,
    utcnow,
)
from apt.domain.platforms import all_platforms
from apt.logging_setup import configure_logging, get_logger
from apt.messaging.consumer import Consumer
from apt.messaging.publisher import Publisher, close_publisher, get_publisher
from apt.observability import metrics
from apt.resilience.bulkhead import BulkheadRegistry
from apt.resilience.circuit_breaker import get_circuit_breaker
from apt.resilience.feature_flags import Flag, get_feature_flags
from apt.resilience.rate_limiter import get_rate_limiter
from apt.resilience.redis_client import close_redis
from apt.resilience.retry import (
    is_retryable_status,
    tier_for_attempt,
    tier_for_retry_after,
)
from apt.worker.sender import PlatformSender, SendResult

logger = get_logger(__name__)

# Teto de adiamentos. Um adiamento nao consome tentativa (ver
# `SendTaskMessage`), entao sem este limite uma campanha configurada muito acima
# da capacidade produziria tarefas circulando entre a fila e o rate limiter
# indefinidamente. 200 e generoso -- com o degrau minimo de 1s, sao mais de tres
# minutos de espera acumulada antes de desistir.
MAX_DEFERS = 200


def build_worker_id() -> str:
    """Identificador desta replica.

    Usa o hostname (que no Docker Compose e o id curto do container) mais um
    sufixo aleatorio. O hostname torna o id reconhecivel no `docker compose ps`;
    o sufixo evita colisao se dois workers rodarem no mesmo host fora do Docker.

    Vai para `executions.worker_id`, e e o que permite provar a distribuicao de
    carga entre replicas em `GET /admin/workers`.
    """
    host = os.environ.get("HOSTNAME") or socket.gethostname()
    return f"worker-{host[:12]}-{uuid4().hex[:4]}"


class Worker:
    """Consumidor que aplica as politicas de resiliencia e envia."""

    def __init__(self) -> None:
        self.worker_id = build_worker_id()
        self._settings = get_settings()
        self._consumer = Consumer(consumer_tag=self.worker_id)
        self._publisher: Publisher = get_publisher()
        self._sender = PlatformSender()
        self._bulkheads = BulkheadRegistry.from_settings()
        self._limiter = get_rate_limiter()
        self._breaker = get_circuit_breaker(observer_id=self.worker_id)
        self._flags = get_feature_flags()

    # =====================================================================
    # Ciclo de vida
    # =====================================================================
    async def start(self) -> None:
        """Conecta, comeca a consumir e bloqueia ate o encerramento."""
        logger.info("worker.starting", worker_id=self.worker_id, version=__version__)

        await self._publisher.connect()
        await self._consumer.connect()
        await self._consumer.start_task_consumers(self.handle_task)
        await self._consumer.start_control_consumer(self.handle_control)

        # Task auxiliar que amostra gauges periodicamente. Gauges precisam ser
        # ATUALIZADOS para refletir a realidade -- ao contrario de counters, que
        # sao incrementados no fluxo. Sem esta task, `apt_rate_limit_tokens`
        # ficaria congelado no ultimo valor observado.
        sampler = asyncio.create_task(self._sample_gauges(), name="apt-gauge-sampler")

        logger.info("worker.started", worker_id=self.worker_id)
        try:
            await self._consumer.wait_until_stopped()
        finally:
            sampler.cancel()
            await self.shutdown()

    async def shutdown(self) -> None:
        """Encerra ordenadamente: drena as tarefas em voo e fecha os recursos."""
        logger.info("worker.stopping", worker_id=self.worker_id)
        # A ordem importa: primeiro o consumer (que espera as tarefas em voo
        # terminarem), depois os recursos que essas tarefas usam. Fechar o
        # publisher antes faria uma tarefa em voo falhar ao tentar reenfileirar.
        await self._consumer.close()
        await self._sender.close()
        await close_publisher()
        await close_redis()
        await dispose_engine()
        logger.info("worker.stopped", worker_id=self.worker_id)

    def request_stop(self) -> None:
        """Sinaliza o encerramento (chamado pelo handler de sinal)."""
        self._consumer.request_stop()

    # =====================================================================
    # Processamento de uma tarefa
    # =====================================================================
    async def handle_task(self, message: SendTaskMessage, raw: AbstractIncomingMessage) -> None:
        """Processa uma tarefa aplicando as cinco camadas em ordem."""
        platform = message.platform

        # --- Camada 1: feature flags (cache local, custo ~zero) -----------
        limiter_on = await self._flags.is_enabled(Flag.RATE_LIMITER_ENABLED)
        breaker_on = await self._flags.is_enabled(Flag.CIRCUIT_BREAKER_ENABLED)

        # Teto de adiamentos: antes de qualquer coisa, para nao gastar recurso
        # com uma tarefa que ja circulou demais.
        if message.defers >= MAX_DEFERS:
            await self._send_to_dlq(
                message,
                outcome=Outcome.RATE_LIMITED_LOCAL,
                error=f"excedeu {MAX_DEFERS} adiamentos sem conseguir enviar",
                reason="max_defers",
            )
            await raw.ack()
            return

        # --- Camada 2: bulkhead (semaforo local) ---------------------------
        bulkhead = self._bulkheads.get(platform)
        if not await bulkhead.acquire():
            metrics.bulkhead_rejections.labels(platform=str(platform)).inc()
            await self._defer(
                message,
                outcome=Outcome.BULKHEAD_FULL,
                retry_after_ms=500,
                reason="bulkhead_full",
            )
            await raw.ack()
            return

        try:
            # --- Camada 3: circuit breaker (1 ida ao Redis) ---------------
            if breaker_on:
                decision = await self._breaker.allow(platform)
                await self._persist_transition(decision, platform, reason="allow")
                if not decision.allowed:
                    await self._defer(
                        message,
                        outcome=Outcome.CIRCUIT_OPEN,
                        retry_after_ms=decision.retry_after_ms,
                        reason=f"circuit_{decision.state}",
                    )
                    await raw.ack()
                    return

            # --- Camada 4: rate limiter (2 idas ao Redis) -----------------
            if limiter_on:
                rl = await self._limiter.acquire(platform, message.content_url)
                metrics.rate_limit_decisions.labels(
                    platform=str(platform),
                    allowed=str(rl.allowed).lower(),
                    limited_by=rl.limited_by or "none",
                ).inc()
                if not rl.allowed:
                    await self._defer(
                        message,
                        outcome=Outcome.RATE_LIMITED_LOCAL,
                        retry_after_ms=rl.retry_after_ms,
                        reason=f"rate_limit_{rl.limited_by}",
                    )
                    await raw.ack()
                    return

            # --- Camada 5: envio -----------------------------------------
            await self._mark_in_flight(message)
            result = await self._sender.send(
                platform, content_url=message.content_url, task_id=message.task_id
            )
            await self._handle_result(message, result, raw)

        finally:
            # `finally` e obrigatorio: um `return` antecipado ou uma excecao sem
            # o release vazaria um slot do compartimento permanentemente, e apos
            # N vazamentos a plataforma pararia de ser atendida por este worker.
            bulkhead.release()
            metrics.bulkhead_in_use.labels(platform=str(platform)).set(bulkhead.stats.in_use)

    async def _handle_result(
        self,
        message: SendTaskMessage,
        result: SendResult,
        raw: AbstractIncomingMessage,
    ) -> None:
        """Registra o resultado do envio e decide ack / retry / DLQ."""
        platform = message.platform
        attempt = message.attempt + 1

        metrics.sends_total.labels(platform=str(platform), outcome=str(result.outcome)).inc()
        if result.latency_ms:
            metrics.send_latency_seconds.labels(platform=str(platform)).observe(
                result.latency_ms / 1000.0
            )

        await self._record_execution(message, result, attempt=attempt)
        self._observe_schedule_delay(message)

        # --- Sucesso ------------------------------------------------------
        if result.outcome.is_success:
            if await self._flags.is_enabled(Flag.CIRCUIT_BREAKER_ENABLED):
                decision = await self._breaker.record_success(platform)
                await self._persist_transition(decision, platform, reason="sucesso")
            async with connection() as conn:
                await TaskRepository.set_status(
                    conn, UUID(message.task_id), TaskStatus.SENT, attempts=attempt
                )
                await CampaignRepository.increment_result(conn, UUID(message.campaign_id), sent=1)
            await raw.ack()
            return

        # --- Rejeicao da plataforma ---------------------------------------
        # Somente `is_platform_rejection` alimenta o breaker. Adiamentos nossos
        # (rate limiter, bulkhead) nao passam por aqui -- se passassem, o rate
        # limiter abriria o circuito ao fazer o proprio trabalho.
        if result.outcome.is_platform_rejection and await self._flags.is_enabled(
            Flag.CIRCUIT_BREAKER_ENABLED
        ):
            decision = await self._breaker.record_failure(platform, reason=str(result.outcome))
            await self._persist_transition(decision, platform, reason=str(result.outcome))
            if decision.transition and decision.transition[1] is BreakerState.OPEN:
                await self._maybe_auto_pause(message)

        # Nao retentavel (4xx que nao 408/429): repetir nao muda o resultado, so
        # gasta cota do rate limiter e atrasa tarefas que teriam sucesso.
        if result.http_status is not None and not is_retryable_status(result.http_status):
            await self._send_to_dlq(
                message,
                outcome=result.outcome,
                error=result.error,
                reason=f"http_{result.http_status}_nao_retentavel",
                attempts=attempt,
            )
            await raw.ack()
            return

        # Esgotou as tentativas.
        if attempt >= self._settings.max_attempts:
            await self._send_to_dlq(
                message,
                outcome=result.outcome,
                error=result.error,
                reason="max_attempts",
                attempts=attempt,
            )
            await raw.ack()
            return

        # Retry com backoff. `Retry-After` da plataforma tem precedencia sobre o
        # nosso calculo: quando ela informa o prazo, e instrucao, nao estimativa.
        if result.retry_after_ms:
            tier = tier_for_retry_after(result.retry_after_ms)
            delay = result.retry_after_ms
        else:
            tier, delay = tier_for_attempt(attempt)

        retried = message.with_attempt(attempt)
        await self._publisher.publish_retry(retried, tier=tier, reason=str(result.outcome))
        metrics.retries_scheduled.labels(
            platform=str(platform), tier=str(tier), reason=str(result.outcome)
        ).inc()

        async with connection() as conn:
            await TaskRepository.set_status(
                conn, UUID(message.task_id), TaskStatus.FAILED, attempts=attempt
            )

        logger.info(
            "worker.retry_scheduled",
            task_id=message.task_id,
            platform=str(platform),
            attempt=attempt,
            tier=tier,
            calculated_delay_ms=delay,
            outcome=str(result.outcome),
        )
        await raw.ack()

    # =====================================================================
    # Adiamento e DLQ
    # =====================================================================
    async def _defer(
        self,
        message: SendTaskMessage,
        *,
        outcome: Outcome,
        retry_after_ms: int,
        reason: str,
    ) -> None:
        """Reenfileira a tarefa sem consumir tentativa.

        Adiamento nao e falha: registramos a execucao com o `Outcome`
        correspondente (para o relatorio poder distinguir autolimitacao de
        bloqueio) e marcamos a tarefa como `deferred`, mas o contador `attempt`
        fica intacto.
        """
        metrics.sends_total.labels(platform=str(message.platform), outcome=str(outcome)).inc()

        await self._record_execution(
            message,
            SendResult(outcome=outcome, error=reason),
            attempt=max(1, message.attempt + 1),
        )

        tier = tier_for_retry_after(max(1, retry_after_ms))
        await self._publisher.publish_retry(message.with_defer(), tier=tier, reason=reason)

        async with connection() as conn:
            await TaskRepository.set_status(conn, UUID(message.task_id), TaskStatus.DEFERRED)

        logger.debug(
            "worker.deferred",
            task_id=message.task_id,
            platform=str(message.platform),
            outcome=str(outcome),
            defers=message.defers + 1,
            retry_after_ms=retry_after_ms,
            tier=tier,
            reason=reason,
        )

    async def _send_to_dlq(
        self,
        message: SendTaskMessage,
        *,
        outcome: Outcome,
        error: str | None,
        reason: str,
        attempts: int | None = None,
    ) -> None:
        """Manda a tarefa para a DLQ e registra em `failures`.

        Duas gravacoes de proposito: a DLQ do RabbitMQ guarda a mensagem (para
        reprocessamento) e a tabela `failures` guarda o contexto consultavel por
        SQL. Depender so da DLQ obrigaria a inspecionar o broker para responder
        "quantas tarefas falharam hoje, por plataforma?".
        """
        total_attempts = attempts if attempts is not None else message.attempt

        await self._publisher.publish_dead(message, reason=reason)
        metrics.tasks_dead.labels(platform=str(message.platform), reason=reason).inc()

        async with connection() as conn:
            await TaskRepository.set_status(
                conn, UUID(message.task_id), TaskStatus.DEAD, attempts=total_attempts
            )
            await FailureRepository.record(
                conn,
                task_id=UUID(message.task_id),
                campaign_id=UUID(message.campaign_id),
                platform=message.platform,
                total_attempts=total_attempts,
                last_outcome=outcome,
                last_error=error,
                payload=dict(message.to_dict()),
            )
            await CampaignRepository.increment_result(conn, UUID(message.campaign_id), failed=1)

        logger.error(
            "worker.task_dead",
            task_id=message.task_id,
            platform=str(message.platform),
            attempts=total_attempts,
            defers=message.defers,
            outcome=str(outcome),
            reason=reason,
            error=error,
        )

    # =====================================================================
    # Persistencia auxiliar
    # =====================================================================
    async def _mark_in_flight(self, message: SendTaskMessage) -> None:
        async with connection() as conn:
            await TaskRepository.set_status(conn, UUID(message.task_id), TaskStatus.IN_FLIGHT)

    async def _record_execution(
        self, message: SendTaskMessage, result: SendResult, *, attempt: int
    ) -> None:
        async with connection() as conn:
            await ExecutionRepository.record(
                conn,
                ExecutionRecord(
                    task_id=UUID(message.task_id),
                    campaign_id=UUID(message.campaign_id),
                    platform=message.platform,
                    attempt=attempt,
                    outcome=result.outcome,
                    http_status=result.http_status,
                    latency_ms=result.latency_ms or None,
                    error_message=result.error,
                    worker_id=self.worker_id,
                    correlation_id=message.correlation_id,
                ),
            )

    async def _persist_transition(self, decision: Any, platform: Platform, *, reason: str) -> None:
        """Grava uma transicao do circuito em `breaker_events`, se houve."""
        if not getattr(decision, "transition", None):
            return
        from_state, to_state = decision.transition
        metrics.circuit_transitions.labels(
            platform=str(platform),
            from_state=str(from_state),
            to_state=str(to_state),
        ).inc()
        metrics.set_circuit_state(str(platform), str(to_state))
        async with connection() as conn:
            await BreakerEventRepository.record(
                conn,
                platform=platform,
                from_state=from_state,
                to_state=to_state,
                reason=reason,
                failure_count=getattr(decision, "failure_count", None),
                observed_by=self.worker_id,
            )

    async def _maybe_auto_pause(self, message: SendTaskMessage) -> None:
        """Pausa a campanha quando o circuito abre, se a flag estiver ligada.

        Desligado por padrao: com ele ativo, o teste de resiliencia pararia de
        gerar trafego assim que o circuito abrisse e nao daria para observar as
        sondas de half_open recuperando o servico.
        """
        if not await self._flags.is_enabled(Flag.AUTO_PAUSE_ON_OPEN):
            return
        from apt.domain.models import CampaignStatus

        async with connection() as conn:
            await CampaignRepository.set_status(
                conn, UUID(message.campaign_id), CampaignStatus.PAUSED
            )
        logger.warning(
            "worker.campaign_auto_paused",
            campaign_id=message.campaign_id,
            platform=str(message.platform),
            reason="circuito aberto e auto_pause_on_open ativa",
        )

    def _observe_schedule_delay(self, message: SendTaskMessage) -> None:
        """Mede o atraso entre o instante planejado e o envio real.

        Quantifica o custo do rate limiter: quanto ele esta atrasando os envios
        para manter a vazao dentro do limite. Falha de parsing e ignorada -- uma
        metrica nao pode derrubar o processamento da tarefa.
        """
        try:
            from datetime import datetime

            scheduled = datetime.fromisoformat(message.scheduled_at)
            delay = (utcnow() - scheduled).total_seconds()
            if delay >= 0:
                metrics.schedule_delay_seconds.labels(platform=str(message.platform)).observe(delay)
        except (ValueError, TypeError):
            pass

    # =====================================================================
    # Controle e metricas
    # =====================================================================
    async def handle_control(self, message: ControlMessage) -> None:
        """Trata um evento vindo do exchange fanout."""
        logger.info("worker.control_received", type=message.type, payload=message.payload)
        if message.type == "flags_changed":
            # A invalidacao explicita e o que faz a mudanca de flag valer na hora,
            # em vez de esperar o TTL de 2s do cache.
            self._flags.invalidate()

    async def _sample_gauges(self) -> None:
        """Atualiza os gauges periodicamente.

        Counters sao incrementados no fluxo; gauges precisam ser amostrados. 5s
        e um intervalo confortavel -- o Prometheus raspa a cada 5s, entao
        amostrar mais rapido nao acrescentaria informacao e adicionaria idas ao
        Redis sem beneficio.
        """
        while True:
            try:
                for platform in all_platforms():
                    tokens = await self._limiter.peek(platform)
                    if tokens is not None:
                        metrics.rate_limit_tokens.labels(platform=str(platform)).set(tokens)

                    snapshot = await self._breaker.snapshot(platform)
                    metrics.set_circuit_state(str(platform), str(snapshot.get("state", "closed")))

                for platform_name, stats in self._bulkheads.snapshot().items():
                    metrics.bulkhead_in_use.labels(platform=platform_name).set(stats["in_use"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("worker.gauge_sampling_failed", error=str(exc))

            await asyncio.sleep(5.0)


async def run() -> None:
    """Ponto de entrada assincrono: configura sinais e sobe o worker."""
    settings = get_settings()
    configure_logging(
        service_name=settings.service_name,
        level=settings.log_level,
        as_json=settings.log_json,
    )

    # Servidor HTTP so para expor /metrics. O worker nao tem API -- esta porta
    # existe para o Prometheus e para o healthcheck do compose.
    start_http_server(settings.metrics_port)
    logger.info("worker.metrics_server_started", port=settings.metrics_port)

    worker = Worker()

    # SIGTERM e o sinal que o `docker compose down` envia. Sem tratar, o processo
    # morre imediatamente e as tarefas em voo sao abandonadas -- o broker as
    # reentrega, mas o envio pode ja ter saido, gerando duplicidade. Com o
    # handler, o worker drena o que esta em voo antes de sair.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, worker.request_stop)
        except NotImplementedError:
            # `add_signal_handler` nao existe no Windows. Em producao o worker
            # roda em container Linux; localmente o Ctrl+C ainda funciona via
            # KeyboardInterrupt, tratado em `main()`.
            logger.debug("worker.signal_handler_unavailable", signal=sig.name)

    await worker.start()


def main() -> None:
    """Ponto de entrada sincrono (`python -m apt.worker.main`)."""
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("worker.interrupted")


if __name__ == "__main__":
    main()