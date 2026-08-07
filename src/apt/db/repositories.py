"""Repositorios: todo o SQL do sistema mora aqui.

Um repositorio por agregado. A regra que sustenta a organizacao: nenhum modulo
fora de `apt.db` escreve SQL. API, worker e scheduler falam com metodos de
repositorio. Quando uma consulta ficar lenta, existe um unico lugar para
procurar.

Os metodos recebem a `AsyncConnection` como parametro em vez de abrir a propria
transacao. Isso permite que quem chama componha varias escritas numa transacao
so -- o dispatcher, por exemplo, cria a tarefa e incrementa o contador da
campanha atomicamente, e um erro no meio nao deixa contador furado.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from apt.domain.models import (
    BreakerState,
    CampaignStatus,
    ExecutionRecord,
    JitterStrategy,
    Outcome,
    Platform,
    TaskStatus,
)


# ---------------------------------------------------------------------------
# Campanhas
# ---------------------------------------------------------------------------
class CampaignRepository:
    """Leitura e escrita de campanhas."""

    @staticmethod
    async def create(
        conn: AsyncConnection,
        *,
        name: str,
        platform: Platform,
        total_sends: int,
        target_rate_per_min: float,
        jitter_strategy: JitterStrategy,
    ) -> UUID:
        """Cria a campanha em `draft` e devolve o id gerado.

        Nasce em `draft` de proposito: a campanha so passa a `active` -- e o
        scheduler so a enxerga -- depois que o pool de conteudos foi cadastrado.
        Se nascesse ativa, haveria uma janela em que o dispatcher tentaria
        materializar tarefas de uma campanha sem nenhuma URL.
        """
        result = await conn.execute(
            text(
                """
                INSERT INTO campaigns
                    (name, platform, total_sends, target_rate_per_min, jitter_strategy, status)
                VALUES
                    (:name, :platform, :total_sends, :rate, :jitter, 'draft')
                RETURNING id
                """
            ),
            {
                "name": name,
                "platform": str(platform),
                "total_sends": total_sends,
                "rate": target_rate_per_min,
                "jitter": str(jitter_strategy),
            },
        )
        return UUID(str(result.scalar_one()))

    @staticmethod
    async def get(conn: AsyncConnection, campaign_id: UUID) -> dict[str, Any] | None:
        result = await conn.execute(
            text(
                """
                SELECT id, name, platform, status, total_sends, target_rate_per_min,
                       jitter_strategy, dispatched_count, sent_count, failed_count,
                       created_at, updated_at, started_at, completed_at
                FROM campaigns
                WHERE id = :id
                """
            ),
            {"id": str(campaign_id)},
        )
        row = result.mappings().first()
        return dict(row) if row else None

    @staticmethod
    async def list_all(
        conn: AsyncConnection,
        *,
        status: CampaignStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        # O filtro opcional e feito com `(:status IS NULL OR status = ...)` em
        # vez de concatenar SQL condicionalmente: uma consulta so, ainda
        # parametrizada, sem risco de injecao.
        result = await conn.execute(
            text(
                """
                SELECT id, name, platform, status, total_sends, target_rate_per_min,
                       jitter_strategy, dispatched_count, sent_count, failed_count,
                       created_at, updated_at, started_at, completed_at
                FROM campaigns
                WHERE (CAST(:status AS campaign_status) IS NULL
                       OR status = CAST(:status AS campaign_status))
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            {
                "status": str(status) if status else None,
                "limit": limit,
                "offset": offset,
            },
        )
        return [dict(r) for r in result.mappings()]

    @staticmethod
    async def claim_active_for_dispatch(
        conn: AsyncConnection, *, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Reserva campanhas ativas para este tick do dispatcher.

        `FOR UPDATE SKIP LOCKED` e o detalhe importante. Se duas instancias da
        API rodarem em paralelo, cada dispatcher travaria as mesmas linhas e uma
        das duas ficaria bloqueada esperando -- ou, pior, ambas materializariam
        as mesmas tarefas e a campanha enviaria em dobro.

        Com SKIP LOCKED, a segunda instancia simplesmente pula as linhas ja
        travadas e trabalha nas outras. O lock vive so ate o fim da transacao
        do tick, que dura milissegundos.

        Ordenar por `updated_at ASC` faz a campanha menos recentemente
        atendida ser servida primeiro: e uma politica round-robin simples que
        impede uma campanha grande de monopolizar todos os ticks.
        """
        result = await conn.execute(
            text(
                """
                SELECT id, name, platform, total_sends, target_rate_per_min,
                       jitter_strategy, dispatched_count, sent_count, updated_at
                FROM campaigns
                WHERE status = 'active'
                  AND dispatched_count < total_sends
                ORDER BY updated_at ASC
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """
            ),
            {"limit": limit},
        )
        return [dict(r) for r in result.mappings()]

    @staticmethod
    async def set_status(conn: AsyncConnection, campaign_id: UUID, status: CampaignStatus) -> bool:
        """Altera o status e devolve True se a campanha existia.

        `started_at` e gravado na primeira ativacao (`COALESCE` preserva o valor
        de uma reativacao posterior, para que pausar/retomar nao reescreva o
        inicio real da campanha).
        """
        result = await conn.execute(
            text(
                """
                UPDATE campaigns
                SET status = CAST(:status AS campaign_status),
                    updated_at = now(),
                    started_at = CASE
                        WHEN :status = 'active' THEN COALESCE(started_at, now())
                        ELSE started_at
                    END,
                    completed_at = CASE
                        WHEN :status IN ('completed', 'failed') THEN now()
                        ELSE completed_at
                    END
                WHERE id = :id
                RETURNING id
                """
            ),
            {"id": str(campaign_id), "status": str(status)},
        )
        return result.first() is not None

    @staticmethod
    async def register_dispatch(conn: AsyncConnection, campaign_id: UUID, count: int) -> None:
        """Soma `count` ao total despachado e completa a campanha se acabou.

        O `CASE` faz a transicao para `completed` no mesmo UPDATE que incrementa
        o contador. Fazer em dois comandos abriria uma janela em que
        `dispatched_count == total_sends` mas o status ainda e `active` -- e o
        proximo tick materializaria tarefas alem do orcamento.
        """
        await conn.execute(
            text(
                """
                UPDATE campaigns
                SET dispatched_count = dispatched_count + :count,
                    updated_at = now(),
                    status = CASE
                        WHEN dispatched_count + :count >= total_sends
                            THEN 'completed'::campaign_status
                        ELSE status
                    END,
                    completed_at = CASE
                        WHEN dispatched_count + :count >= total_sends THEN now()
                        ELSE completed_at
                    END
                WHERE id = :id
                """
            ),
            {"id": str(campaign_id), "count": count},
        )

    @staticmethod
    async def increment_result(
        conn: AsyncConnection, campaign_id: UUID, *, sent: int = 0, failed: int = 0
    ) -> None:
        """Atualiza os contadores desnormalizados de resultado.

        Sao contadores redundantes (o valor exato sai de `send_tasks`), mas
        evitam um `COUNT(*)` a cada consulta de status. Ver docs/TRADE-OFFS.md.
        """
        if sent == 0 and failed == 0:
            return
        await conn.execute(
            text(
                """
                UPDATE campaigns
                SET sent_count = sent_count + :sent,
                    failed_count = failed_count + :failed,
                    updated_at = now()
                WHERE id = :id
                """
            ),
            {"id": str(campaign_id), "sent": sent, "failed": failed},
        )


# ---------------------------------------------------------------------------
# Pool de conteudos
# ---------------------------------------------------------------------------
class ContentRepository:
    """Pool de URLs rotativas de uma campanha."""

    @staticmethod
    async def add_many(
        conn: AsyncConnection,
        campaign_id: UUID,
        contents: list[tuple[str, int]],
    ) -> int:
        """Cadastra URLs no pool. Recebe pares `(url, peso)`.

        `ON CONFLICT DO UPDATE` torna a operacao idempotente: reenviar a mesma
        lista atualiza os pesos em vez de estourar com violacao de unicidade.
        """
        if not contents:
            return 0
        inserted = 0
        for url, weight in contents:
            await conn.execute(
                text(
                    """
                    INSERT INTO campaign_contents (campaign_id, content_url, weight)
                    VALUES (:cid, :url, :weight)
                    ON CONFLICT (campaign_id, content_url)
                    DO UPDATE SET weight = EXCLUDED.weight
                    """
                ),
                {"cid": str(campaign_id), "url": url, "weight": weight},
            )
            inserted += 1
        return inserted

    @staticmethod
    async def list_for_campaign(conn: AsyncConnection, campaign_id: UUID) -> list[dict[str, Any]]:
        result = await conn.execute(
            text(
                """
                SELECT id, content_url, weight, sends_count
                FROM campaign_contents
                WHERE campaign_id = :cid
                ORDER BY created_at ASC
                """
            ),
            {"cid": str(campaign_id)},
        )
        return [dict(r) for r in result.mappings()]

    @staticmethod
    async def take_next(conn: AsyncConnection, campaign_id: UUID) -> dict[str, Any] | None:
        """Escolhe o proximo conteudo do pool e registra o uso.

        A rotacao e um round-robin ponderado suave: escolhemos a URL com o menor
        valor de `sends_count / weight`. Um conteudo de peso 2 acumula "credito"
        na metade da velocidade de um de peso 1, entao recebe o dobro de envios
        ao longo do tempo -- sem precisar manter indice de rodizio nem estado
        na aplicacao.

        A alternativa obvia (sortear aleatoriamente) daria a distribuicao certa
        na media, mas com desvio visivel em campanhas curtas: e perfeitamente
        possivel sortear a mesma URL cinco vezes seguidas, que e exatamente o
        padrao de concentracao que queremos evitar.

        `UPDATE ... RETURNING` num unico comando resolve escolha e incremento
        atomicamente: sem isso, dois dispatchers concorrentes leriam a mesma
        URL antes de qualquer incremento.
        """
        result = await conn.execute(
            text(
                """
                UPDATE campaign_contents
                SET sends_count = sends_count + 1
                WHERE id = (
                    SELECT id
                    FROM campaign_contents
                    WHERE campaign_id = :cid
                    ORDER BY (sends_count::numeric / weight) ASC, created_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, content_url, weight, sends_count
                """
            ),
            {"cid": str(campaign_id)},
        )
        row = result.mappings().first()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Tarefas de envio
# ---------------------------------------------------------------------------
class TaskRepository:
    """Envios individuais materializados pelo scheduler."""

    @staticmethod
    async def create(
        conn: AsyncConnection,
        *,
        campaign_id: UUID,
        content_id: UUID,
        platform: Platform,
        content_url: str,
        scheduled_at: Any,
        correlation_id: str,
    ) -> UUID:
        result = await conn.execute(
            text(
                """
                INSERT INTO send_tasks
                    (campaign_id, content_id, platform, content_url,
                     scheduled_at, correlation_id, status)
                VALUES
                    (:cid, :content_id, :platform, :url, :scheduled_at, :corr, 'pending')
                RETURNING id
                """
            ),
            {
                "cid": str(campaign_id),
                "content_id": str(content_id),
                "platform": str(platform),
                "url": content_url,
                "scheduled_at": scheduled_at,
                "corr": correlation_id,
            },
        )
        return UUID(str(result.scalar_one()))

    @staticmethod
    async def set_status(
        conn: AsyncConnection,
        task_id: UUID,
        status: TaskStatus,
        *,
        attempts: int | None = None,
    ) -> None:
        await conn.execute(
            text(
                """
                UPDATE send_tasks
                SET status = CAST(:status AS task_status),
                    attempts = COALESCE(:attempts, attempts),
                    updated_at = now()
                WHERE id = :id
                """
            ),
            {"id": str(task_id), "status": str(status), "attempts": attempts},
        )

    @staticmethod
    async def status_breakdown(conn: AsyncConnection, campaign_id: UUID) -> dict[str, int]:
        """Conta as tarefas por status. Alimenta `GET /campaigns/{id}/status`."""
        result = await conn.execute(
            text(
                """
                SELECT status::text AS status, COUNT(*) AS total
                FROM send_tasks
                WHERE campaign_id = :cid
                GROUP BY status
                """
            ),
            {"cid": str(campaign_id)},
        )
        return {r["status"]: int(r["total"]) for r in result.mappings()}

    @staticmethod
    async def get(conn: AsyncConnection, task_id: UUID) -> dict[str, Any] | None:
        result = await conn.execute(
            text(
                """
                SELECT id, campaign_id, content_id, platform, content_url,
                       status, scheduled_at, attempts, correlation_id, created_at
                FROM send_tasks
                WHERE id = :id
                """
            ),
            {"id": str(task_id)},
        )
        row = result.mappings().first()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Execucoes (tentativas)
# ---------------------------------------------------------------------------
class ExecutionRepository:
    """Historico de tentativas -- a fonte das latencias do relatorio."""

    @staticmethod
    async def record(conn: AsyncConnection, execution: ExecutionRecord) -> None:
        await conn.execute(
            text(
                """
                INSERT INTO executions
                    (task_id, campaign_id, platform, attempt, outcome,
                     http_status, latency_ms, error_message, worker_id, correlation_id)
                VALUES
                    (:task_id, :campaign_id, :platform, :attempt, :outcome,
                     :http_status, :latency_ms, :error_message, :worker_id, :correlation_id)
                """
            ),
            {
                "task_id": str(execution.task_id),
                "campaign_id": str(execution.campaign_id),
                "platform": str(execution.platform),
                "attempt": execution.attempt,
                "outcome": str(execution.outcome),
                "http_status": execution.http_status,
                "latency_ms": execution.latency_ms,
                # Erros de plataforma podem trazer HTML inteiro de pagina de
                # erro. Truncamos para nao inflar a tabela.
                "error_message": (execution.error_message or None)
                and execution.error_message[:500],
                "worker_id": execution.worker_id,
                "correlation_id": execution.correlation_id,
            },
        )

    @staticmethod
    async def latency_percentiles(
        conn: AsyncConnection, *, platform: Platform | None = None
    ) -> dict[str, float | None]:
        """Percentis de latencia dos envios bem-sucedidos.

        Filtramos por `outcome = 'sent'` porque misturar timeouts na amostra
        distorceria o p99: um timeout de 5s registra 5000ms e nao representa a
        latencia de servico, mas o teto que nos mesmos configuramos.

        `percentile_cont` interpola entre os pontos vizinhos, o que da um
        percentil mais estavel que `percentile_disc` em amostras pequenas.
        """
        result = await conn.execute(
            text(
                """
                SELECT
                    COUNT(*)                                                        AS samples,
                    percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms)        AS p50,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)        AS p95,
                    percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)        AS p99,
                    AVG(latency_ms)                                                 AS avg,
                    MAX(latency_ms)                                                 AS max
                FROM executions
                WHERE outcome = 'sent'
                  AND latency_ms IS NOT NULL
                  AND (CAST(:platform AS TEXT) IS NULL OR platform = CAST(:platform AS TEXT))
                """
            ),
            {"platform": str(platform) if platform else None},
        )
        row = result.mappings().one()
        return {
            "samples": int(row["samples"] or 0),
            "p50": float(row["p50"]) if row["p50"] is not None else None,
            "p95": float(row["p95"]) if row["p95"] is not None else None,
            "p99": float(row["p99"]) if row["p99"] is not None else None,
            "avg": float(row["avg"]) if row["avg"] is not None else None,
            "max": float(row["max"]) if row["max"] is not None else None,
        }

    @staticmethod
    async def outcome_breakdown(
        conn: AsyncConnection, *, platform: Platform | None = None
    ) -> dict[str, int]:
        """Contagem por resultado. E a tabela central do relatorio de testes."""
        result = await conn.execute(
            text(
                """
                SELECT outcome, COUNT(*) AS total
                FROM executions
                WHERE (CAST(:platform AS TEXT) IS NULL OR platform = CAST(:platform AS TEXT))
                GROUP BY outcome
                ORDER BY total DESC
                """
            ),
            {"platform": str(platform) if platform else None},
        )
        return {r["outcome"]: int(r["total"]) for r in result.mappings()}

    @staticmethod
    async def worker_distribution(conn: AsyncConnection) -> dict[str, int]:
        """Quantos envios cada worker atendeu.

        E a evidencia do padrao Load Balancing: com prefetch=1 e N replicas, a
        distribuicao deve ficar aproximadamente uniforme. Um worker com 90% dos
        envios indicaria prefetch alto demais ou replicas travadas.
        """
        result = await conn.execute(
            text(
                """
                SELECT COALESCE(worker_id, 'desconhecido') AS worker_id,
                       COUNT(*) AS total
                FROM executions
                GROUP BY worker_id
                ORDER BY total DESC
                """
            )
        )
        return {r["worker_id"]: int(r["total"]) for r in result.mappings()}

    @staticmethod
    async def throughput_per_second(
        conn: AsyncConnection, *, platform: Platform, window_seconds: int = 60
    ) -> list[dict[str, Any]]:
        """Envios aceitos por segundo, na janela recente.

        Esta e a consulta que prova a tese central da POC: o pico de
        `sent_per_second` tem de ficar abaixo do `allowed_rps` configurado,
        independentemente de quantos workers estejam rodando.

        `date_trunc('second', ...)` agrupa por segundo cheio -- e a mesma
        granularidade em que o token bucket opera.
        """
        result = await conn.execute(
            text(
                """
                SELECT date_trunc('second', created_at) AS second,
                       COUNT(*) AS sent
                FROM executions
                WHERE platform = :platform
                  AND outcome = 'sent'
                  AND created_at > now() - make_interval(secs => :window)
                GROUP BY 1
                ORDER BY 1
                """
            ),
            {"platform": str(platform), "window": window_seconds},
        )
        return [
            {"second": r["second"].isoformat(), "sent": int(r["sent"])} for r in result.mappings()
        ]


# ---------------------------------------------------------------------------
# Falhas terminais (DLQ)
# ---------------------------------------------------------------------------
class FailureRepository:
    """Tarefas que esgotaram as tentativas."""

    @staticmethod
    async def record(
        conn: AsyncConnection,
        *,
        task_id: UUID,
        campaign_id: UUID,
        platform: Platform,
        total_attempts: int,
        last_outcome: Outcome,
        last_error: str | None,
        payload: dict[str, Any],
    ) -> None:
        await conn.execute(
            text(
                """
                INSERT INTO failures
                    (task_id, campaign_id, platform, total_attempts,
                     last_outcome, last_error, payload)
                VALUES
                    (:task_id, :campaign_id, :platform, :attempts,
                     :outcome, :error, CAST(:payload AS jsonb))
                """
            ),
            {
                "task_id": str(task_id),
                "campaign_id": str(campaign_id),
                "platform": str(platform),
                "attempts": total_attempts,
                "outcome": str(last_outcome),
                "error": (last_error or None) and last_error[:500],
                # O payload original vai inteiro para a coluna JSONB: e o que
                # permite reprocessar a tarefa depois pelo endpoint de replay.
                "payload": json.dumps(payload),
            },
        )

    @staticmethod
    async def list_unresolved(conn: AsyncConnection, *, limit: int = 100) -> list[dict[str, Any]]:
        result = await conn.execute(
            text(
                """
                SELECT id, task_id, campaign_id, platform, total_attempts,
                       last_outcome, last_error, payload, created_at
                FROM failures
                WHERE resolved = false
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        return [dict(r) for r in result.mappings()]

    @staticmethod
    async def mark_resolved(conn: AsyncConnection, failure_id: int) -> bool:
        result = await conn.execute(
            text("UPDATE failures SET resolved = true WHERE id = :id RETURNING id"),
            {"id": failure_id},
        )
        return result.first() is not None


# ---------------------------------------------------------------------------
# Eventos do circuit breaker
# ---------------------------------------------------------------------------
class BreakerEventRepository:
    """Historico de transicoes do circuito.

    Existe para a demonstracao: sem esta tabela, provar que o circuito abriu e
    fechou durante o teste de resiliencia dependeria de encontrar as linhas
    certas no log de um dos workers.
    """

    @staticmethod
    async def record(
        conn: AsyncConnection,
        *,
        platform: Platform,
        from_state: BreakerState,
        to_state: BreakerState,
        reason: str | None = None,
        failure_count: int | None = None,
        observed_by: str | None = None,
    ) -> None:
        await conn.execute(
            text(
                """
                INSERT INTO breaker_events
                    (platform, from_state, to_state, reason, failure_count, observed_by)
                VALUES
                    (:platform, CAST(:from_state AS breaker_state),
                     CAST(:to_state AS breaker_state), :reason, :failures, :observed_by)
                """
            ),
            {
                "platform": str(platform),
                "from_state": str(from_state),
                "to_state": str(to_state),
                "reason": reason,
                "failures": failure_count,
                "observed_by": observed_by,
            },
        )

    @staticmethod
    async def recent(conn: AsyncConnection, *, limit: int = 50) -> list[dict[str, Any]]:
        result = await conn.execute(
            text(
                """
                SELECT platform, from_state::text AS from_state,
                       to_state::text AS to_state, reason, failure_count,
                       observed_by, created_at
                FROM breaker_events
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        return [dict(r) for r in result.mappings()]


# ---------------------------------------------------------------------------
# Thresholds das plataformas
# ---------------------------------------------------------------------------
class PlatformRepository:
    """Thresholds por plataforma, ajustaveis em runtime.

    Ficam no banco -- e nao apenas no `.env` -- porque os limites reais de uma
    plataforma mudam sem aviso. Ajustar um threshold nao deve exigir redeploy.
    """

    @staticmethod
    async def list_all(conn: AsyncConnection) -> list[dict[str, Any]]:
        result = await conn.execute(
            text(
                """
                SELECT platform, allowed_rps, burst_capacity,
                       estimated_limit_rps, notes, updated_at
                FROM platform_thresholds
                ORDER BY platform
                """
            )
        )
        return [dict(r) for r in result.mappings()]

    @staticmethod
    async def get(conn: AsyncConnection, platform: Platform) -> dict[str, Any] | None:
        result = await conn.execute(
            text(
                """
                SELECT platform, allowed_rps, burst_capacity,
                       estimated_limit_rps, notes, updated_at
                FROM platform_thresholds
                WHERE platform = :platform
                """
            ),
            {"platform": str(platform)},
        )
        row = result.mappings().first()
        return dict(row) if row else None

    @staticmethod
    async def update(
        conn: AsyncConnection,
        platform: Platform,
        *,
        allowed_rps: float | None = None,
        burst_capacity: int | None = None,
    ) -> dict[str, Any] | None:
        """Atualiza o threshold. `COALESCE` faz cada campo ser opcional."""
        result = await conn.execute(
            text(
                """
                UPDATE platform_thresholds
                SET allowed_rps    = COALESCE(:rps, allowed_rps),
                    burst_capacity = COALESCE(:burst, burst_capacity),
                    updated_at     = now()
                WHERE platform = :platform
                RETURNING platform, allowed_rps, burst_capacity,
                          estimated_limit_rps, notes, updated_at
                """
            ),
            {
                "platform": str(platform),
                "rps": allowed_rps,
                "burst": burst_capacity,
            },
        )
        row = result.mappings().first()
        return dict(row) if row else None
