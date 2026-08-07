"""Scheduler: materializa campanhas em tarefas individuais e as publica.

Roda como background task da API (`asyncio.create_task` no lifespan), nao como
container proprio -- ver ADR-010 para a justificativa e o custo dessa escolha.

O CICLO DE UM TICK

    1. reserva as campanhas ativas   (FOR UPDATE SKIP LOCKED)
    2. para cada campanha:
       a. planeja o tick             (jitter.plan_tick)
       b. gira o pool de URLs        (ContentRepository.take_next)
       c. cria as linhas em send_tasks
       d. publica as mensagens no RabbitMQ
       e. atualiza dispatched_count
    3. dorme ate o proximo tick

A SEPARACAO ENTRE "MATERIALIZAR" E "ENVIAR"

O dispatcher nao envia nada. Ele decide QUANTAS e QUANDO, grava a intencao no
banco e entrega a fila. Quem envia -- e quem aplica rate limit, breaker e
bulkhead -- e o worker.

Essa separacao e o que permite escalar as duas coisas de forma independente e,
mais importante, e o que faz o sistema absorver picos: se a plataforha
desacelerar, as tarefas acumulam na fila em vez de estourar em erro.

CONSISTENCIA: A ORDEM ENTRE BANCO E FILA

Gravamos no banco ANTES de publicar. A ordem importa e as duas falhas possiveis
sao assimetricas:

    banco primeiro, publish falha
        -> existe uma linha em `send_tasks` com status `pending` que nunca sera
           consumida. Uma tarefa orfa, visivel e auditavel.

    publish primeiro, banco falha
        -> o worker recebe uma mensagem cujo `task_id` nao existe em
           `send_tasks`. Ele nao consegue registrar a execucao nem atualizar
           status; a tarefa e enviada e desaparece do registro.

Preferimos a primeira: registro visivel sem envio e melhor que envio sem
registro. A solucao definitiva seria o padrao Transactional Outbox, que ficou
fora do escopo -- decisao registrada em docs/TRADE-OFFS.md.
"""

from __future__ import annotations

import asyncio
import random
from contextlib import suppress
from datetime import timedelta
from typing import Any
from uuid import UUID

from apt.config import get_settings
from apt.db.engine import connection
from apt.db.repositories import CampaignRepository, ContentRepository, TaskRepository
from apt.domain.models import (
    JitterStrategy,
    Platform,
    SendTaskMessage,
    utcnow,
)
from apt.logging_setup import bind_correlation_id, get_logger, new_correlation_id
from apt.messaging.publisher import Publisher
from apt.observability import metrics
from apt.resilience.feature_flags import Flag, get_feature_flags
from apt.scheduling.jitter import plan_tick

logger = get_logger(__name__)


class Dispatcher:
    """Loop que materializa campanhas ativas em tarefas de envio."""

    def __init__(self, publisher: Publisher) -> None:
        self._publisher = publisher
        self._settings = get_settings()
        self._flags = get_feature_flags()
        self._stopping = asyncio.Event()
        self._rng = random.Random()
        self._ticks = 0

    async def run(self) -> None:
        """Loop principal. Roda ate `stop()` ser chamado.

        A rede de seguranca em volta do `tick()` e essencial: se uma excecao
        escapar deste loop, a background task morre em silencio e o sistema
        deixa de gerar tarefas -- sem nenhum erro visivel, apenas campanhas
        ativas que nunca enviam nada. Foi o comportamento observado na primeira
        versao, e o try/except existe para que uma falha transitoria de banco
        nao mate o scheduler.
        """
        logger.info(
            "dispatcher.started",
            tick_seconds=self._settings.dispatch_tick_seconds,
            max_batch=self._settings.dispatch_max_batch,
        )
        while not self._stopping.is_set():
            try:
                await self.tick()
            except Exception as exc:
                logger.exception("dispatcher.tick_failed", error=str(exc))

            # Esperamos no EVENTO de parada com timeout, em vez de `sleep`. A
            # diferenca aparece no shutdown: com `sleep`, o processo teria de
            # aguardar o tick corrente terminar de dormir antes de encerrar.
            # Aqui, `stop()` acorda a espera na hora.
            #
            # O timeout e o caminho NORMAL do loop (significa "hora do proximo
            # tick"), nao uma excecao -- por isso o `suppress`.
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=self._settings.dispatch_tick_seconds
                )

        logger.info("dispatcher.stopped", ticks=self._ticks)

    def stop(self) -> None:
        """Sinaliza o encerramento do loop."""
        self._stopping.set()

    async def tick(self) -> int:
        """Executa um tick. Devolve quantas tarefas foram publicadas."""
        self._ticks += 1

        if not await self._flags.is_enabled(Flag.DISPATCH_ENABLED):
            # Flag de pausa global. As campanhas seguem `active` -- so paramos de
            # materializar. Util para drenar a fila antes de uma medicao limpa.
            return 0

        jitter_enabled = await self._flags.is_enabled(Flag.JITTER_ENABLED)
        total_published = 0

        # Uma transacao por tick. As campanhas reservadas ficam travadas apenas
        # durante ela -- milissegundos -- e o commit no fim garante que contador
        # e tarefas sao gravados juntos.
        async with connection() as conn:
            campaigns = await CampaignRepository.claim_active_for_dispatch(conn, limit=20)
            if not campaigns:
                return 0

            for campaign in campaigns:
                published = await self._dispatch_campaign(
                    conn, campaign, jitter_enabled=jitter_enabled
                )
                total_published += published

        if total_published:
            logger.info(
                "dispatcher.tick_completed",
                tick=self._ticks,
                campaigns=len(campaigns),
                published=total_published,
                jitter_enabled=jitter_enabled,
            )
        return total_published

    async def _dispatch_campaign(
        self, conn: Any, campaign: dict[str, Any], *, jitter_enabled: bool
    ) -> int:
        """Materializa e publica as tarefas de uma campanha neste tick."""
        campaign_id = UUID(str(campaign["id"]))
        platform = Platform(str(campaign["platform"]))
        strategy = JitterStrategy(str(campaign["jitter_strategy"]))

        remaining = int(campaign["total_sends"]) - int(campaign["dispatched_count"])
        if remaining <= 0:
            return 0

        now = utcnow()
        plan = plan_tick(
            strategy=strategy,
            target_rate_per_min=float(campaign["target_rate_per_min"]),
            tick_seconds=self._settings.dispatch_tick_seconds,
            hour_utc=now.hour,
            remaining_budget=remaining,
            max_batch=self._settings.dispatch_max_batch,
            jitter_enabled=jitter_enabled,
            rng=self._rng,
        )
        if plan.count == 0:
            return 0

        published = 0
        for offset_ms in plan.offsets_ms:
            # Cada tarefa recebe o proprio id de correlacao. Isso permite seguir
            # um envio especifico pelos logs da API e de todos os workers -- sem
            # ele, so daria para filtrar pela campanha inteira.
            correlation_id = new_correlation_id()
            bind_correlation_id(correlation_id)

            content = await ContentRepository.take_next(conn, campaign_id)
            if content is None:
                # Campanha ativa sem nenhuma URL no pool. Nao ha o que enviar.
                # WARNING e nao ERROR: e um erro de configuracao do usuario, nao
                # uma falha do sistema.
                logger.warning(
                    "dispatcher.no_content",
                    campaign_id=str(campaign_id),
                    note="campanha ativa sem URLs cadastradas no pool",
                )
                break

            scheduled_at = now + timedelta(milliseconds=offset_ms)

            # 1) grava a intencao (ver nota sobre a ordem no docstring do modulo)
            task_id = await TaskRepository.create(
                conn,
                campaign_id=campaign_id,
                content_id=UUID(str(content["id"])),
                platform=platform,
                content_url=str(content["content_url"]),
                scheduled_at=scheduled_at,
                correlation_id=correlation_id,
            )

            # 2) publica
            message = SendTaskMessage(
                task_id=str(task_id),
                campaign_id=str(campaign_id),
                content_id=str(content["id"]),
                platform=platform,
                content_url=str(content["content_url"]),
                correlation_id=correlation_id,
                scheduled_at=scheduled_at.isoformat(),
                attempt=0,
            )
            await self._publisher.publish_task(message)

            metrics.tasks_dispatched.labels(platform=str(platform)).inc()
            published += 1

        if published:
            await CampaignRepository.register_dispatch(conn, campaign_id, published)

        logger.debug(
            "dispatcher.campaign_dispatched",
            campaign_id=str(campaign_id),
            platform=str(platform),
            strategy=str(strategy),
            planned=plan.count,
            published=published,
            mean_interval_ms=round(plan.mean_interval_ms, 1),
        )
        return published

    @property
    def ticks(self) -> int:
        """Quantos ticks foram executados. Exposto em `/health/ready`."""
        return self._ticks