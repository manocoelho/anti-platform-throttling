"""Modelos de dominio: enums e estruturas puras, sem dependencia de I/O.

Este modulo nao importa banco, Redis nem HTTP de proposito. Ele e o vocabulario
compartilhado entre API, worker e scheduler, e pode ser importado num teste
unitario sem subir nenhuma infraestrutura.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID


class Platform(StrEnum):
    """Plataformas externas suportadas.

    `StrEnum` (e nao `Enum`) porque estes valores atravessam varias fronteiras
    como texto: routing key do RabbitMQ, chave do Redis, coluna do Postgres e
    label do Prometheus. Com StrEnum, `Platform.YOUTUBE == "youtube"` e
    verdadeiro e nao precisamos de `.value` espalhado pelo codigo.
    """

    YOUTUBE = "youtube"
    INSTAGRAM = "instagram"


class CampaignStatus(StrEnum):
    """Ciclo de vida de uma campanha.

    draft -> active -> completed
               |
               +-> paused -> active
               +-> failed
    """

    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskStatus(StrEnum):
    """Estado de um envio individual.

    `DEFERRED` merece atencao: significa que NOS adiamos a tarefa (rate
    limiter, circuito aberto ou bulkhead cheio). Nao e uma falha -- e o sistema
    funcionando como projetado. Contabilizar adiamento como falha inflaria a
    taxa de erro e, pior, abriria o circuit breaker sem que a plataforma
    tivesse reclamado de nada.
    """

    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    SENT = "sent"
    DEFERRED = "deferred"
    FAILED = "failed"
    DEAD = "dead"  # esgotou as tentativas e foi para a DLQ


class BreakerState(StrEnum):
    """Estados do circuit breaker.

    CLOSED     - trafego normal, contando falhas
    OPEN       - recusando envios; nem tenta chamar a plataforma
    HALF_OPEN  - admitindo poucas sondas para descobrir se ja recuperou
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class Outcome(StrEnum):
    """Resultado de uma tentativa de envio.

    A distincao central e entre o que a PLATAFORMA respondeu e o que NOS
    decidimos:

    Resposta da plataforma:
        SENT       - aceito (2xx)
        THROTTLED  - 429: passamos do limite. Todo THROTTLED e uma falha nossa
                     de calibragem, e o numero que a POC quer levar a zero.
        ERROR      - 5xx ou resposta inesperada
        TIMEOUT    - nao respondeu no prazo

    Decisao nossa (a requisicao nao saiu):
        RATE_LIMITED_LOCAL - o token bucket negou
        CIRCUIT_OPEN       - o circuito da plataforma esta aberto
        BULKHEAD_FULL      - sem slot de concorrencia para esta plataforma

    Manter os dois grupos separados e o que permite afirmar na apresentacao
    "nos nos autolimitamos N vezes e fomos bloqueados 0 vezes".
    """

    SENT = "sent"
    THROTTLED = "throttled"
    ERROR = "error"
    TIMEOUT = "timeout"
    RATE_LIMITED_LOCAL = "rate_limited_local"
    CIRCUIT_OPEN = "circuit_open"
    BULKHEAD_FULL = "bulkhead_full"

    @property
    def is_success(self) -> bool:
        return self is Outcome.SENT

    @property
    def is_platform_rejection(self) -> bool:
        """True quando a plataforma nos rejeitou -- o que o circuito observa.

        Somente estes resultados contam para abrir o circuit breaker. Os
        adiamentos internos (`is_self_throttled`) nao contam: se contassem, o
        proprio rate limiter abriria o circuito ao fazer o seu trabalho.
        """
        return self in {Outcome.THROTTLED, Outcome.ERROR, Outcome.TIMEOUT}

    @property
    def is_self_throttled(self) -> bool:
        """True quando nos mesmos barramos o envio antes de sair."""
        return self in {
            Outcome.RATE_LIMITED_LOCAL,
            Outcome.CIRCUIT_OPEN,
            Outcome.BULKHEAD_FULL,
        }


class JitterStrategy(StrEnum):
    """Estrategias de distribuicao temporal dos envios.

    UNIFORM     - intervalo sorteado uniformemente em torno da media.
                  Simples e previsivel.
    EXPONENTIAL - intervalos com distribuicao exponencial (processo de
                  Poisson). Estatisticamente e o que mais se parece com
                  chegadas independentes de usuarios reais.
    HUMANIZED   - exponencial modulada por um perfil de atividade diario
                  (menos volume de madrugada). O padrao do projeto.
    """

    UNIFORM = "uniform"
    EXPONENTIAL = "exponential"
    HUMANIZED = "humanized"


def coerce_int(value: object, default: int = 0) -> int:
    """Converte um valor de payload em inteiro, com fallback silencioso.

    Existe porque os campos numericos de `SendTaskMessage` chegam de JSON
    desserializado (`dict[str, object]`) e podem vir como `int`, `str`, `None` --
    ou ausentes, se a mensagem foi publicada por uma versao anterior do sistema e
    ainda estava na fila durante um deploy.

    Recusamos ser estritos aqui de proposito: estourar faria uma tarefa
    perfeitamente valida ir para a DLQ por causa de um campo de metadado. O
    default e o comportamento seguro -- `attempt=0` significa "trate como
    primeira tentativa", que no maximo concede um retry extra.
    """
    if value is None:
        return default
    try:
        return int(value)  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return default


def utcnow() -> datetime:
    """Agora, com timezone UTC explicito.

    Existe para que nenhum ponto do codigo use `datetime.utcnow()`, que devolve
    um datetime ingenuo (sem tzinfo) e produz comparacoes silenciosamente
    erradas contra os `TIMESTAMPTZ` do Postgres.
    """
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class SendTaskMessage:
    """Payload que viaja pelo RabbitMQ da API ate o worker.

    `frozen=True` porque um worker nunca deve alterar a mensagem que recebeu --
    a unica mutacao legitima e `with_attempt()`, que produz uma copia nova ao
    reenfileirar.

    Este e o contrato entre dois processos: mudar um campo aqui exige pensar em
    mensagens antigas ainda na fila durante o deploy. Por isso o
    `from_dict()` trata campos ausentes com default em vez de estourar.
    """

    task_id: str
    campaign_id: str
    content_id: str
    platform: Platform
    content_url: str
    correlation_id: str
    scheduled_at: str  # ISO-8601; texto para serializar em JSON sem conversao

    # DOIS CONTADORES, E A DISTINCAO ENTRE ELES E ESSENCIAL
    #
    # `attempt` conta TENTATIVAS DE ENVIO que falharam: a requisicao saiu e a
    # plataforma recusou (429, 5xx, timeout). Ao atingir `APT_MAX_ATTEMPTS`, a
    # tarefa vai para a DLQ.
    #
    # `defers` conta ADIAMENTOS: nos mesmos decidimos nao enviar (rate limiter
    # negou, circuito aberto, bulkhead cheio). A requisicao nunca saiu.
    #
    # Por que nao usar um contador so: num sistema saudavel sob carga, os
    # adiamentos sao FREQUENTES e ESPERADOS -- e exatamente o rate limiter
    # fazendo o seu trabalho. Se eles incrementassem `attempt`, uma tarefa
    # adiada 4 vezes iria para a DLQ sem NUNCA ter sido enviada, e o sistema
    # descartaria trabalho legitimo justamente quando estivesse se protegendo
    # corretamente. Foi o primeiro bug que apareceu ao juntar rate limiter e
    # retry, e a separacao dos contadores e a correcao.
    attempt: int = 0
    defers: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "campaign_id": self.campaign_id,
            "content_id": self.content_id,
            "platform": str(self.platform),
            "content_url": self.content_url,
            "correlation_id": self.correlation_id,
            "scheduled_at": self.scheduled_at,
            "attempt": self.attempt,
            "defers": self.defers,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SendTaskMessage:
        return cls(
            task_id=str(data["task_id"]),
            campaign_id=str(data["campaign_id"]),
            content_id=str(data["content_id"]),
            platform=Platform(str(data["platform"])),
            content_url=str(data["content_url"]),
            correlation_id=str(data.get("correlation_id", "")),
            scheduled_at=str(data["scheduled_at"]),
            # `coerce_int` com default: mensagens publicadas por uma versao
            # anterior do sistema podem nao ter estes campos e ainda estar na
            # fila durante um deploy. Estourar aqui as mandaria todas para a DLQ.
            attempt=coerce_int(data.get("attempt")),
            defers=coerce_int(data.get("defers")),
        )

    def _replace(self, *, attempt: int, defers: int) -> SendTaskMessage:
        return SendTaskMessage(
            task_id=self.task_id,
            campaign_id=self.campaign_id,
            content_id=self.content_id,
            platform=self.platform,
            content_url=self.content_url,
            correlation_id=self.correlation_id,
            scheduled_at=self.scheduled_at,
            attempt=attempt,
            defers=defers,
        )

    def with_attempt(self, attempt: int) -> SendTaskMessage:
        """Copia com o contador de TENTATIVAS atualizado.

        Usado ao reenfileirar apos falha real de envio. O contador viaja no
        payload para que qualquer worker -- nao necessariamente o que falhou --
        saiba em que ponto do backoff a tarefa esta.
        """
        return self._replace(attempt=attempt, defers=self.defers)

    def with_defer(self) -> SendTaskMessage:
        """Copia com o contador de ADIAMENTOS incrementado.

        `attempt` fica intacto de proposito -- ver o comentario nos campos.
        """
        return self._replace(attempt=self.attempt, defers=self.defers + 1)


@dataclass(frozen=True, slots=True)
class ControlMessage:
    """Evento de controle publicado no exchange fanout.

    Fanout, e nao topic: uma invalidacao de feature flag precisa chegar a
    *todas* as replicas de worker, nao a uma delas. Com um exchange do tipo
    topic e uma fila compartilhada, apenas um worker receberia o aviso e os
    outros seguiriam com o cache velho.

    Tipos usados:
        flags_changed     - alguem alterou uma feature flag; limpe o cache
        campaign_paused   - pare de processar esta campanha
        campaign_resumed  - volte a processar
    """

    type: str
    payload: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {"type": self.type, "payload": self.payload}

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ControlMessage:
        raw = data.get("payload") or {}
        return cls(
            type=str(data["type"]),
            payload=dict(raw) if isinstance(raw, dict) else {},
        )


@dataclass(slots=True)
class ExecutionRecord:
    """Uma tentativa de envio, pronta para ser persistida em `executions`.

    Mutavel (sem `frozen`) porque o worker a monta em etapas: cria antes do
    envio, preenche latencia e status depois da resposta.
    """

    task_id: UUID
    campaign_id: UUID
    platform: Platform
    attempt: int
    outcome: Outcome
    http_status: int | None = None
    latency_ms: int | None = None
    error_message: str | None = None
    worker_id: str | None = None
    correlation_id: str = ""
