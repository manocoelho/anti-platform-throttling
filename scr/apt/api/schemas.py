"""Schemas Pydantic: o contrato HTTP da API.

Estes modelos fazem tres coisas de uma vez, e e por isso que valem o arquivo
separado:

1. Validam a entrada antes de qualquer codigo nosso rodar. Um
   `target_rate_per_min` negativo e rejeitado com 422 e mensagem clara, em vez de
   virar uma divisao estranha dentro do `jitter.plan_tick`.
2. Documentam a API. O FastAPI gera o OpenAPI a partir daqui -- os `description`
   e `examples` abaixo sao o que aparece no `/docs`.
3. Definem a forma da resposta, evitando devolver acidentalmente uma coluna
   interna do banco que ninguem deveria ver.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from apt.domain.models import (
    BreakerState,
    CampaignStatus,
    JitterStrategy,
    Platform,
)

# ---------------------------------------------------------------------------
# Campanhas
# ---------------------------------------------------------------------------


class ContentIn(BaseModel):
    """Um item do pool de conteudos rotativos."""

    url: Annotated[str, Field(min_length=1, max_length=2000)]
    weight: Annotated[int, Field(ge=1, le=100)] = 1

    model_config = ConfigDict(
        json_schema_extra={"example": {"url": "https://youtube.com/watch?v=exemplo1", "weight": 2}}
    )


class CampaignCreate(BaseModel):
    """Corpo de `POST /campaigns`."""

    name: Annotated[str, Field(min_length=1, max_length=200)]
    platform: Platform
    total_sends: Annotated[int, Field(ge=1, le=1_000_000)]
    target_rate_per_min: Annotated[float, Field(gt=0, le=100_000)]
    jitter_strategy: JitterStrategy = JitterStrategy.HUMANIZED

    # Pelo menos uma URL e obrigatoria: uma campanha sem pool de conteudos nao
    # tem o que enviar, e o dispatcher apenas emitiria um WARNING a cada tick.
    # Barrar na entrada e melhor que aceitar uma campanha inerte.
    contents: Annotated[list[ContentIn], Field(min_length=1, max_length=500)]

    # Ativar imediatamente apos criar. Padrao True porque e o caminho comum;
    # False permite montar a campanha e ativar depois.
    activate: bool = True

    @field_validator("contents")
    @classmethod
    def _reject_duplicate_urls(cls, value: list[ContentIn]) -> list[ContentIn]:
        """Recusa URLs repetidas no mesmo pool.

        O banco tem `UNIQUE (campaign_id, content_url)` e o `ON CONFLICT` do
        repositorio faria a segunda ocorrencia sobrescrever silenciosamente o
        peso da primeira. O usuario que mandou a mesma URL com pesos diferentes
        provavelmente errou; e melhor dizer isso do que escolher um dos pesos por
        ele.
        """
        seen: set[str] = set()
        duplicates: set[str] = set()
        for item in value:
            if item.url in seen:
                duplicates.add(item.url)
            seen.add(item.url)
        if duplicates:
            raise ValueError(f"URLs duplicadas no pool: {sorted(duplicates)}")
        return value

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Lancamento - video institucional",
                "platform": "youtube",
                "total_sends": 500,
                "target_rate_per_min": 600,
                "jitter_strategy": "humanized",
                "contents": [
                    {"url": "https://youtube.com/watch?v=exemplo1", "weight": 2},
                    {"url": "https://youtube.com/watch?v=exemplo2", "weight": 1},
                ],
                "activate": True,
            }
        }
    )


class ContentOut(BaseModel):
    """Um conteudo do pool, com o uso acumulado."""

    id: UUID
    content_url: str
    weight: int
    sends_count: int


class CampaignOut(BaseModel):
    """Representacao de uma campanha nas respostas."""

    id: UUID
    name: str
    platform: Platform
    status: CampaignStatus
    total_sends: int
    target_rate_per_min: float
    jitter_strategy: JitterStrategy
    dispatched_count: int
    sent_count: int
    failed_count: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None


class CampaignStatusOut(BaseModel):
    """Resposta de `GET /campaigns/{id}/status`.

    Reune numeros de tres origens (campanha, tarefas e execucoes) para que o
    operador nao precise fazer tres chamadas para entender o andamento.
    """

    campaign: CampaignOut
    contents: list[ContentOut]
    # Contagem de tarefas por status: {"pending": 12, "sent": 400, ...}
    task_breakdown: dict[str, int]
    # Contagem de tentativas por resultado: {"sent": 400, "throttled": 0, ...}
    outcome_breakdown: dict[str, int]
    # Percentual concluido, so para nao obrigar o cliente a calcular.
    progress_percent: float


class CampaignActionOut(BaseModel):
    """Resposta das acoes de pausar/retomar/ativar."""

    id: UUID
    status: CampaignStatus
    message: str


# ---------------------------------------------------------------------------
# Plataformas
# ---------------------------------------------------------------------------


class PlatformOut(BaseModel):
    """Threshold e estado de uma plataforma."""

    platform: Platform
    allowed_rps: float = Field(description="Vazao que NOS nos permitimos.")
    burst_capacity: int = Field(description="Capacidade do token bucket (rajada maxima).")
    estimated_limit_rps: float | None = Field(
        default=None,
        description=(
            "Limite ESTIMADO da plataforma. Nao e um numero oficial -- ver "
            "docs/adr/ADR-008-simulador-de-plataformas.md."
        ),
    )
    safety_margin_percent: float = Field(
        description="Percentual do limite estimado que deixamos de usar como folga."
    )
    notes: str | None = None
    # Estado ao vivo, lido do Redis.
    available_tokens: float | None = None
    circuit_state: BreakerState | None = None


class PlatformThresholdUpdate(BaseModel):
    """Corpo de `PATCH /platforms/{platform}`. Ambos os campos sao opcionais."""

    allowed_rps: float | None = Field(default=None, gt=0, le=100_000)
    burst_capacity: int | None = Field(default=None, ge=1, le=100_000)


# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------


class FlagsOut(BaseModel):
    """Todas as flags e seus valores."""

    flags: dict[str, bool]


class FlagUpdate(BaseModel):
    """Corpo de `PATCH /flags/{flag}`."""

    value: bool

    model_config = ConfigDict(json_schema_extra={"example": {"value": False}})


# ---------------------------------------------------------------------------
# Administracao
# ---------------------------------------------------------------------------


class FailureOut(BaseModel):
    """Uma tarefa que esgotou as tentativas."""

    id: int
    task_id: UUID
    campaign_id: UUID
    platform: Platform
    total_attempts: int
    last_outcome: str
    last_error: str | None = None
    created_at: datetime


class BreakerEventOut(BaseModel):
    """Uma transicao registrada do circuit breaker."""

    platform: Platform
    from_state: BreakerState
    to_state: BreakerState
    reason: str | None = None
    failure_count: int | None = None
    observed_by: str | None = None
    created_at: datetime


class LatencyStatsOut(BaseModel):
    """Percentis de latencia dos envios bem-sucedidos."""

    platform: Platform | None = None
    samples: int
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None
    avg: float | None = None
    max: float | None = None


class ThroughputPointOut(BaseModel):
    """Envios aceitos num segundo especifico."""

    second: str
    sent: int


class ResetOut(BaseModel):
    """Resposta das acoes de reset."""

    removed: int
    message: str


# ---------------------------------------------------------------------------
# Saude
# ---------------------------------------------------------------------------


class HealthOut(BaseModel):
    """Resposta dos endpoints de saude.

    A distincao entre `/health/live` e `/health/ready` e a mesma do Kubernetes:

    live  -- "o processo esta vivo?" Nao checa dependencia. Se responder erro, a
             resposta correta e REINICIAR o container.
    ready -- "posso receber trafego?" Checa Postgres e Redis. Se responder erro,
             a resposta correta e PARAR DE MANDAR TRAFEGO -- reiniciar nao
             resolveria, porque o problema esta numa dependencia.

    Confundir os dois causa o comportamento classico de reiniciar a aplicacao em
    loop porque o banco caiu.
    """

    status: str
    service: str
    version: str
    checks: dict[str, bool] = Field(default_factory=dict)
    details: dict[str, str | int | float | None] = Field(default_factory=dict)
