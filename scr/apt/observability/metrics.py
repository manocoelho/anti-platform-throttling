"""Metricas Prometheus do sistema.

Todas as metricas sao declaradas aqui, num modulo unico. Duas razoes: o
`prometheus_client` mantem um registro global e declarar a mesma metrica duas
vezes estoura em runtime; e concentrar as declaracoes torna possivel ler a lista
completa do que o sistema observa num lugar so.

CONVENCAO DE NOMES

Prefixo `apt_`, unidade no sufixo (`_seconds`, `_total`), e o tipo escolhido
conforme a pergunta:

    Counter    -- so cresce. "quantos envios ja aconteceram?"
    Gauge      -- sobe e desce. "quantas fichas ha no balde agora?"
    Histogram  -- distribuicao. "qual a latencia p95?"

CUIDADO COM CARDINALIDADE

Labels multiplicam series temporais: 2 plataformas x 7 resultados = 14 series
para `apt_sends_total`. Isso e barato.

O que NAO fazemos: usar `content_url`, `task_id` ou `campaign_id` como label.
Cada valor distinto criaria uma serie temporal permanente no Prometheus, e uma
campanha com mil URLs geraria mil series -- que continuariam consumindo memoria
depois da campanha terminar. Esse dado pertence ao Postgres, que e feito para
alta cardinalidade; o Prometheus responde perguntas agregadas.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.core import REGISTRY

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
tasks_dispatched = Counter(
    "apt_tasks_dispatched_total",
    "Tarefas materializadas e publicadas na fila pelo scheduler.",
    ["platform"],
)

# ---------------------------------------------------------------------------
# Envios
# ---------------------------------------------------------------------------
sends_total = Counter(
    "apt_sends_total",
    (
        "Tentativas de envio por resultado. O label `outcome` distingue "
        "rejeicao da plataforma (throttled/error/timeout) de autolimitacao "
        "nossa (rate_limited_local/circuit_open/bulkhead_full) -- e a metrica "
        "central da POC."
    ),
    ["platform", "outcome"],
)

send_latency_seconds = Histogram(
    "apt_send_latency_seconds",
    "Latencia das requisicoes as plataformas (somente envios que sairam).",
    ["platform"],
    # Buckets ajustados ao cenario: o simulador responde em 5-40ms, e o timeout
    # e 5s. Os buckets padrao do prometheus_client comecam em 5ms e vao a 10s,
    # o que deixaria quase toda a amostra no primeiro bucket e tornaria o p95
    # inutilizavel.
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# Mede o atraso entre o `scheduled_at` planejado pelo scheduler e o envio real.
# E a metrica que quantifica o custo do rate limiter: quanto ele esta atrasando
# os envios para manter a vazao dentro do limite.
schedule_delay_seconds = Histogram(
    "apt_schedule_delay_seconds",
    "Atraso entre o instante planejado (scheduled_at) e o envio efetivo.",
    ["platform"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0),
)

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------
rate_limit_decisions = Counter(
    "apt_rate_limit_decisions_total",
    "Decisoes do rate limiter. `limited_by` indica qual eixo negou.",
    ["platform", "allowed", "limited_by"],
)

rate_limit_tokens = Gauge(
    "apt_rate_limit_tokens",
    "Fichas disponiveis no balde da plataforma (amostrado periodicamente).",
    ["platform"],
)

# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------
# Gauge numerico porque o Prometheus so armazena numeros: 0=closed, 1=half_open,
# 2=open. A ordem e crescente em gravidade, entao `max()` sobre as plataformas
# responde "qual o pior estado agora?" -- util para alerta.
circuit_state = Gauge(
    "apt_circuit_state",
    "Estado do circuito por plataforma: 0=closed, 1=half_open, 2=open.",
    ["platform"],
)

circuit_transitions = Counter(
    "apt_circuit_transitions_total",
    "Transicoes de estado do circuit breaker.",
    ["platform", "from_state", "to_state"],
)

# ---------------------------------------------------------------------------
# Bulkhead
# ---------------------------------------------------------------------------
bulkhead_in_use = Gauge(
    "apt_bulkhead_in_use",
    "Slots de concorrencia ocupados por plataforma neste worker.",
    ["platform"],
)

bulkhead_rejections = Counter(
    "apt_bulkhead_rejections_total",
    "Envios recusados por falta de slot no compartimento da plataforma.",
    ["platform"],
)

# ---------------------------------------------------------------------------
# Retry e DLQ
# ---------------------------------------------------------------------------
retries_scheduled = Counter(
    "apt_retries_scheduled_total",
    "Tarefas reenfileiradas para nova tentativa, por degrau de backoff.",
    ["platform", "tier", "reason"],
)

tasks_dead = Counter(
    "apt_tasks_dead_total",
    "Tarefas que esgotaram as tentativas e foram para a DLQ.",
    ["platform", "reason"],
)

# ---------------------------------------------------------------------------
# Simulador de plataformas
# ---------------------------------------------------------------------------
sim_requests = Counter(
    "apt_sim_requests_total",
    "Requisicoes recebidas pelo simulador, por plataforma e status devolvido.",
    ["platform", "status"],
)

sim_active_faults = Gauge(
    "apt_sim_active_faults",
    "1 quando ha injecao de falha ativa para a plataforma, 0 caso contrario.",
    ["platform"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_STATE_TO_NUMBER = {"closed": 0, "half_open": 1, "open": 2}


def set_circuit_state(platform: str, state: str) -> None:
    """Converte o estado textual do circuito no gauge numerico.

    Estado desconhecido vira -1 em vez de estourar: uma metrica nunca deve
    derrubar o caminho de execucao que a produz. O valor negativo e visivelmente
    anomalo num grafico, o que sinaliza o problema sem causar dano.
    """
    circuit_state.labels(platform=platform).set(_STATE_TO_NUMBER.get(state, -1))


def render_metrics(registry: CollectorRegistry | None = None) -> bytes:
    """Serializa o registro no formato de exposicao do Prometheus.

    Usado pelos endpoints `/metrics` da API, do worker e do simulador.
    """
    return generate_latest(registry or REGISTRY)


CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
