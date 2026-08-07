-- ---------------------------------------------------------------------------
-- Anti-Platform Throttling - schema inicial
--
-- Este arquivo e montado em /docker-entrypoint-initdb.d dentro do container do
-- Postgres e roda automaticamente na primeira inicializacao do volume.
-- Ver ADR-012 para a justificativa de usar SQL puro em vez de Alembic.
--
-- Modelo em quatro niveis:
--   campaigns          -> a intencao do administrador ("mande N engajamentos")
--   campaign_contents  -> o pool de URLs rotativas daquela campanha
--   send_tasks         -> cada envio individual que o scheduler materializou
--   executions         -> cada TENTATIVA de envio (1 tarefa -> N tentativas)
--
-- A separacao entre send_tasks e executions e o que permite responder
-- "quantas vezes tentamos?" sem perder o estado final da tarefa.
-- ---------------------------------------------------------------------------

-- gen_random_uuid() vem desta extensao no Postgres 12 e anteriores; no 13+ ela
-- ja e nativa, mas manter o CREATE EXTENSION torna o script portavel.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- Tipos enumerados
--
-- Usamos ENUM em vez de VARCHAR livre para que o banco rejeite um estado
-- invalido. O custo e que adicionar um valor novo exige ALTER TYPE.
-- ---------------------------------------------------------------------------
CREATE TYPE campaign_status AS ENUM ('draft', 'active', 'paused', 'completed', 'failed');
CREATE TYPE task_status     AS ENUM ('pending', 'in_flight', 'sent', 'deferred', 'failed', 'dead');
CREATE TYPE breaker_state   AS ENUM ('closed', 'open', 'half_open');

-- ---------------------------------------------------------------------------
-- platform_thresholds
--
-- Limites estimados por plataforma. Ficam no banco (e nao apenas no codigo)
-- para que o administrador possa ajustar sem redeploy -- os limites reais de
-- uma plataforma mudam sem aviso.
-- ---------------------------------------------------------------------------
CREATE TABLE platform_thresholds (
    platform            TEXT PRIMARY KEY,
    -- Vazao sustentada que NOS nos permitimos (deve ficar abaixo do limite real).
    allowed_rps         NUMERIC(10, 2) NOT NULL CHECK (allowed_rps > 0),
    -- Capacidade do token bucket = tamanho maximo de rajada.
    burst_capacity      INTEGER        NOT NULL CHECK (burst_capacity > 0),
    -- Limite estimado da plataforma, so para referencia/relatorio.
    estimated_limit_rps NUMERIC(10, 2),
    notes               TEXT,
    updated_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);

COMMENT ON TABLE platform_thresholds IS
    'Thresholds por plataforma. allowed_rps e o que aplicamos; estimated_limit_rps e a estimativa do limite da plataforma.';

-- ---------------------------------------------------------------------------
-- campaigns
-- ---------------------------------------------------------------------------
CREATE TABLE campaigns (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT            NOT NULL,
    platform            TEXT            NOT NULL REFERENCES platform_thresholds (platform),
    status              campaign_status NOT NULL DEFAULT 'draft',

    -- Quantos envios a campanha deve fazer no total.
    total_sends         INTEGER         NOT NULL CHECK (total_sends > 0),
    -- Vazao alvo. O scheduler usa este numero para decidir quantas tarefas
    -- materializar por tick; o rate limiter e o teto final independente disto.
    target_rate_per_min NUMERIC(10, 2)  NOT NULL CHECK (target_rate_per_min > 0),

    -- Estrategia de distribuicao temporal: 'uniform' | 'exponential' | 'humanized'
    jitter_strategy     TEXT            NOT NULL DEFAULT 'humanized',

    -- Contadores desnormalizados. Guardar aqui evita um COUNT(*) em send_tasks
    -- a cada tick do scheduler e a cada consulta de status. O trade-off e ter
    -- de manter a consistencia na aplicacao (ver docs/TRADE-OFFS.md).
    dispatched_count    INTEGER         NOT NULL DEFAULT 0,
    sent_count          INTEGER         NOT NULL DEFAULT 0,
    failed_count        INTEGER         NOT NULL DEFAULT 0,

    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,

    CONSTRAINT campaigns_jitter_strategy_check
        CHECK (jitter_strategy IN ('uniform', 'exponential', 'humanized'))
);

-- O scheduler roda esta consulta a cada tick: "quais campanhas estao ativas?".
-- Indice parcial porque so o subconjunto 'active' interessa -- fica pequeno e
-- quente em cache mesmo com muitas campanhas historicas.
CREATE INDEX campaigns_active_idx ON campaigns (platform, updated_at)
    WHERE status = 'active';

-- ---------------------------------------------------------------------------
-- campaign_contents
--
-- Pool de conteudos rotativos: multiplas URLs por campanha. O scheduler gira
-- este pool para nao concentrar todo o volume numa unica URL -- concentrar e
-- justamente o padrao que dispara a deteccao das plataformas.
-- ---------------------------------------------------------------------------
CREATE TABLE campaign_contents (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id   UUID    NOT NULL REFERENCES campaigns (id) ON DELETE CASCADE,
    content_url   TEXT    NOT NULL,
    -- Peso relativo na rotacao: peso 2 recebe o dobro de envios do peso 1.
    weight        INTEGER NOT NULL DEFAULT 1 CHECK (weight > 0),
    -- Quantos envios este conteudo ja recebeu (usado pela rotacao ponderada).
    sends_count   INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (campaign_id, content_url)
);

CREATE INDEX campaign_contents_campaign_idx ON campaign_contents (campaign_id);

-- ---------------------------------------------------------------------------
-- send_tasks
--
-- Uma linha por envio individual materializado pelo scheduler.
-- ---------------------------------------------------------------------------
CREATE TABLE send_tasks (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id    UUID        NOT NULL REFERENCES campaigns (id) ON DELETE CASCADE,
    content_id     UUID        NOT NULL REFERENCES campaign_contents (id) ON DELETE CASCADE,
    platform       TEXT        NOT NULL,
    content_url    TEXT        NOT NULL,
    status         task_status NOT NULL DEFAULT 'pending',

    -- Quando o scheduler decidiu que este envio deveria sair (com jitter
    -- aplicado). Comparar scheduled_at com o sent_at da ultima execucao mede
    -- o atraso real introduzido pelo rate limiter.
    scheduled_at   TIMESTAMPTZ NOT NULL,
    attempts       INTEGER     NOT NULL DEFAULT 0,

    -- Correlaciona a tarefa com todos os logs e mensagens que ela gerou.
    correlation_id TEXT        NOT NULL,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX send_tasks_campaign_status_idx ON send_tasks (campaign_id, status);
CREATE INDEX send_tasks_correlation_idx     ON send_tasks (correlation_id);
-- Usado pelos relatorios de carga: "quantos envios por segundo, por plataforma?"
CREATE INDEX send_tasks_platform_created_idx ON send_tasks (platform, created_at DESC);

-- ---------------------------------------------------------------------------
-- executions
--
-- Uma linha por TENTATIVA. Uma tarefa que sofreu 3 retries tem 3 linhas aqui.
-- E daqui que saem as latencias (p50/p95/p99) do relatorio de testes.
-- ---------------------------------------------------------------------------
CREATE TABLE executions (
    id             BIGSERIAL PRIMARY KEY,
    task_id        UUID        NOT NULL REFERENCES send_tasks (id) ON DELETE CASCADE,
    campaign_id    UUID        NOT NULL REFERENCES campaigns (id) ON DELETE CASCADE,
    platform       TEXT        NOT NULL,
    attempt        INTEGER     NOT NULL CHECK (attempt > 0),

    -- Resultado normalizado: 'sent' | 'throttled' | 'error' | 'timeout'
    -- | 'rate_limited_local' | 'circuit_open' | 'bulkhead_full'
    -- Os tres ultimos representam decisoes NOSSAS -- a requisicao nem chegou
    -- a sair. Separa-los e o que permite provar que o sistema se autolimitou
    -- em vez de ter sido bloqueado pela plataforma.
    outcome        TEXT        NOT NULL,
    http_status    INTEGER,
    latency_ms     INTEGER,
    error_message  TEXT,

    -- Qual worker atendeu. Comprova a distribuicao de carga entre replicas.
    worker_id      TEXT,
    correlation_id TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX executions_task_idx              ON executions (task_id);
CREATE INDEX executions_platform_outcome_idx  ON executions (platform, outcome, created_at DESC);
CREATE INDEX executions_created_idx           ON executions (created_at DESC);
CREATE INDEX executions_worker_idx            ON executions (worker_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- failures
--
-- Tarefas que esgotaram as tentativas e foram para a DLQ. Tabela separada
-- porque e a fila de trabalho do operador ("o que precisa de atencao?"),
-- e nao um log historico.
-- ---------------------------------------------------------------------------
CREATE TABLE failures (
    id             BIGSERIAL PRIMARY KEY,
    task_id        UUID        NOT NULL REFERENCES send_tasks (id) ON DELETE CASCADE,
    campaign_id    UUID        NOT NULL REFERENCES campaigns (id) ON DELETE CASCADE,
    platform       TEXT        NOT NULL,
    total_attempts INTEGER     NOT NULL,
    last_outcome   TEXT        NOT NULL,
    last_error     TEXT,
    payload        JSONB       NOT NULL,
    resolved       BOOLEAN     NOT NULL DEFAULT false,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX failures_unresolved_idx ON failures (created_at DESC) WHERE resolved = false;

-- ---------------------------------------------------------------------------
-- breaker_events
--
-- Historico de transicoes do circuit breaker. Sem esta tabela, provar na
-- apresentacao que o circuito abriu e fechou dependeria de ler log.
-- ---------------------------------------------------------------------------
CREATE TABLE breaker_events (
    id             BIGSERIAL PRIMARY KEY,
    platform       TEXT          NOT NULL,
    from_state     breaker_state NOT NULL,
    to_state       breaker_state NOT NULL,
    reason         TEXT,
    failure_count  INTEGER,
    observed_by    TEXT,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX breaker_events_platform_idx ON breaker_events (platform, created_at DESC);

-- ---------------------------------------------------------------------------
-- Seed: as duas plataformas simuladas
--
-- Instagram: allowed_rps fica em 80% do limite estimado. A margem de 20%
-- absorve o desalinhamento entre a nossa janela de contagem e a da
-- plataforma: mesmo que as duas janelas nao coincidam, nao estouramos o
-- limite dela.
--
-- YouTube: allowed_rps fica em 60% do limite estimado (3 de 5), uma margem
-- MAIOR que os 20% do Instagram. A razao nao e so seguranca de janela: o
-- YouTube foi recalibrado para ficar abaixo do teto de vazao agregada da VM
-- de teste (~6-8 req/s medidos), senao o hardware do ambiente vira o
-- gargalo antes do rate limiter e o mecanismo deixa de ser observavel nos
-- testes de carga/escala. Ver a nota "RECALIBRACAO DO YOUTUBE" em
-- src/apt/domain/platforms.py e TRADE-OFFS.md. O Instagram NAO mudou -- o
-- confundidor so afetava o perfil cujo allowed_rps chegava perto do teto do
-- ambiente.
--
-- burst_capacity segue a formula burst + allowed_rps <= estimated_limit_rps
-- (pior caso: bucket cheio + refill do mesmo segundo, numa unica janela de
-- 1s do simulador). YouTube: 1+3=4 <= 5. Instagram: 1+8=9 <= 10. Ver
-- src/apt/domain/platforms.py e TRADE-OFFS.md item 16 -- os valores
-- originais (16 e 8, contra limites 20 e 10) violavam essa formula por
-- construcao (16+16=32 > 20).
-- ---------------------------------------------------------------------------
INSERT INTO platform_thresholds (platform, allowed_rps, burst_capacity, estimated_limit_rps, notes) VALUES
    ('youtube',   3.0, 1, 5.0,
     'Recalibrado para ficar abaixo do teto de vazao agregada da VM de teste (~6-8 req/s), nao so do limite estimado da plataforma -- ver TRADE-OFFS.md e src/apt/domain/platforms.py. burst_capacity = 1 para manter burst+allowed_rps <= limite estimado.'),
    ('instagram',  8.0, 1, 10.0,
     'Estimativa para ambiente controlado. Limite mantido intencionalmente diferente do YouTube, para exercitar o bulkhead com plataformas assimetricas. burst_capacity = 1 para manter burst+allowed_rps <= limite estimado.');
