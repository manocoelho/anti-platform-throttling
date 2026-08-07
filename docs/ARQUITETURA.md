# Arquitetura — Anti-Platform Throttling

Diagramas C4 nos níveis **1 (Contexto)**, **2 (Containers)** e **3 (Componentes)**.
O documento da disciplina exige os níveis 1 e 2; o nível 3 foi incluído porque é
onde o mecanismo central da POC fica visível.

> **Mudanças em relação ao Projeto 02.** Os diagramas do Projeto 02 previam 4
> containers (API, RabbitMQ, Worker, PostgreSQL). Esta entrega tem 7: entraram
> **Redis** (ADR-003, o estado distribuído que faltava), **Platform Simulator**
> (ADR-008) e **Prometheus**. O scheduler que apareceu durante a implementação foi
> absorvido pela API (ADR-010). Justificativa completa em
> [ATUALIZACOES-DOC-INICIAL.md](ATUALIZACOES-DOC-INICIAL.md).

---

## Nível 1 — Contexto

```mermaid
graph TB
    admin["<b>Administrador</b><br/><i>[Pessoa]</i><br/><br/>Cadastra campanhas,<br/>acompanha execução<br/>e ajusta thresholds"]

    apt["<b>Anti-Platform Throttling</b><br/><i>[Sistema de Software]</i><br/><br/>Controla o envio de requisições<br/>a plataformas externas, distribuindo<br/>a carga no tempo e respeitando<br/>limites de vazão desconhecidos"]

    plataformas["<b>Plataformas Externas</b><br/><i>[Sistema externo — simulado]</i><br/><br/>YouTube e Instagram.<br/>Impõem rate limiting e<br/>respondem 429 ao exceder"]

    admin -->|"cria e monitora campanhas<br/>HTTPS / REST"| apt
    apt -->|"envia requisições de engajamento<br/>de forma controlada<br/>HTTPS"| plataformas
    plataformas -.->|"429 + Retry-After<br/>quando o limite é excedido"| apt

    classDef pessoa fill:#08427b,stroke:#052e56,color:#fff
    classDef sistema fill:#1168bd,stroke:#0b4884,color:#fff
    classDef externo fill:#999999,stroke:#6b6b6b,color:#fff
    class admin pessoa
    class apt sistema
    class plataformas externo
```

**O problema.** Plataformas externas impõem limites de utilização que **não são
publicados** e mudam sem aviso. Exceder esses limites causa throttling e, na
reincidência, penalização algorítmica — e cada requisição enviada *durante* a
penalidade tende a estendê-la.

**A responsabilidade do sistema.** Distribuir a carga ao longo do tempo, manter a
vazão abaixo de um limite estimado com margem de segurança, e reagir a falhas sem
agravá-las.

**O que está fora.** O sistema não descobre os limites reais das plataformas — ver
[ADR-008](adr/ADR-008-simulador-de-plataformas.md) para a discussão dessa fronteira
e o porquê de ela existir.

---

## Nível 2 — Containers

```mermaid
graph TB
    admin["<b>Administrador</b><br/><i>[Pessoa]</i>"]

    subgraph sistema["Anti-Platform Throttling"]
        api["<b>API + Scheduler</b><br/><i>[Container: Python / FastAPI]</i><br/><br/>REST para campanhas, flags e<br/>thresholds. O <b>dispatcher</b> roda<br/>como background task: materializa<br/>campanhas em tarefas e aplica jitter"]

        worker["<b>Worker</b> ×N<br/><i>[Container: Python / aio-pika]</i><br/><br/>Consome a fila e aplica<br/>bulkhead → circuit breaker →<br/>rate limiter antes de cada envio"]

        broker[("<b>RabbitMQ</b><br/><i>[Container]</i><br/><br/>topic + fanout<br/>DLX/DLQ<br/>3 filas de retry com TTL")]

        redis[("<b>Redis</b><br/><i>[Container]</i><br/><br/>token buckets<br/>estado do circuito<br/>feature flags")]

        pg[("<b>PostgreSQL</b><br/><i>[Container]</i><br/><br/>campanhas, pool de URLs,<br/>tarefas, execuções,<br/>falhas, eventos do breaker")]

        prom["<b>Prometheus</b><br/><i>[Container]</i><br/><br/>Coleta /metrics dos três<br/>processos Python"]
    end

    sim["<b>Platform Simulator</b><br/><i>[Container: Python / FastAPI]</i><br/><br/>Simula YouTube e Instagram.<br/>Janela deslizante real,<br/>429 + Retry-After,<br/>injeção de falhas"]

    admin -->|"HTTPS / REST"| api

    api -->|"lê e escreve<br/>SQL / asyncpg"| pg
    api -->|"flags, peek dos buckets<br/>RESP"| redis
    api -->|"publica tarefas e<br/>eventos de controle<br/>AMQP"| broker

    broker -->|"entrega tarefas<br/>prefetch=1, ack manual<br/>AMQP"| worker
    broker -.->|"fanout: invalida flags<br/>em TODAS as réplicas"| worker

    worker -->|"consulta bucket e circuito<br/>scripts Lua atômicos<br/>RESP"| redis
    worker -->|"grava execuções e falhas<br/>SQL / asyncpg"| pg
    worker -->|"envia engajamento<br/>HTTP"| sim
    worker -.->|"reenfileira retry/adiamento<br/>AMQP"| broker

    prom -.->|"raspa /metrics<br/>descoberta por DNS"| api
    prom -.->|"raspa /metrics"| worker
    prom -.->|"raspa /metrics"| sim

    classDef pessoa fill:#08427b,stroke:#052e56,color:#fff
    classDef container fill:#438dd5,stroke:#2e6295,color:#fff
    classDef db fill:#438dd5,stroke:#2e6295,color:#fff
    classDef externo fill:#999999,stroke:#6b6b6b,color:#fff
    class admin pessoa
    class api,worker,prom container
    class broker,redis,pg db
    class sim externo
```

### Responsabilidade de cada container

| Container | Tecnologia | Responsabilidade | Escala |
|---|---|---|---|
| **API + Scheduler** | FastAPI, uvicorn | REST de campanhas/flags/thresholds; o dispatcher materializa campanhas em tarefas com jitter | horizontal (`SKIP LOCKED` evita duplicidade — [ADR-010](adr/ADR-010-scheduler-na-api.md)) |
| **Worker** | aio-pika, httpx | Aplica as 5 camadas de proteção e envia | **horizontal — é a demo central** |
| **RabbitMQ** | 3.13 | Desacopla recebimento de processamento; DLQ; backoff via TTL | single-node na POC |
| **Redis** | 7 | Estado **compartilhado** dos mecanismos distribuídos | single-node na POC |
| **PostgreSQL** | 16 | Persistência e a base das métricas de teste | single-node na POC |
| **Platform Simulator** | FastAPI | Dublê das plataformas com 429 real | 1 instância |
| **Prometheus** | 2.55 | Coleta de métricas | 1 instância |

### Por que o Redis é indispensável

É a decisão que separa um sistema que funciona de um que falha ao escalar:

```
Rate limiter EM MEMÓRIA DE PROCESSO:
  1 worker  × 16 req/s =  16 req/s   ✓  dentro do limite
  5 workers × 16 req/s =  80 req/s   ✗  4× o limite da plataforma

Rate limiter COM ESTADO NO REDIS:
  1 worker  →  16 req/s   ✓
  5 workers →  16 req/s   ✓  o limite é GLOBAL
```

O bug da primeira versão só apareceria **ao escalar** — sob carga, quando o limite
mais importa, e nunca em desenvolvimento com um worker.

---

## Nível 3 — Componentes do Worker

O nível 3 do worker é onde a POC realmente acontece: as cinco camadas de proteção e a
ordem entre elas.

```mermaid
graph TB
    broker[("RabbitMQ<br/>apt.tasks.&lt;plataforma&gt;")]

    subgraph worker["Container: Worker"]
        consumer["<b>Consumer</b><br/><i>messaging/consumer.py</i><br/><br/>prefetch=1, ack manual,<br/>shutdown com drenagem"]

        flags["<b>1. Feature Flags</b><br/><i>resilience/feature_flags.py</i><br/><br/>cache local 2s +<br/>invalidação por fanout<br/><b>custo: ~zero</b>"]

        bulkhead["<b>2. Bulkhead</b><br/><i>resilience/bulkhead.py</i><br/><br/>Semáforo por plataforma<br/>fail-fast em 2s<br/><b>custo: memória local</b>"]

        breaker["<b>3. Circuit Breaker</b><br/><i>resilience/circuit_breaker.py</i><br/><br/>Estado compartilhado<br/>closed / open / half_open<br/><b>custo: 1 ida ao Redis</b>"]

        limiter["<b>4. Rate Limiter</b><br/><i>resilience/rate_limiter.py</i><br/><br/>Token bucket em Lua<br/>eixo conteúdo → eixo plataforma<br/><b>custo: 2 idas ao Redis</b>"]

        sender["<b>5. Sender</b><br/><i>worker/sender.py</i><br/><br/>Pool HTTP por plataforma<br/>traduz resposta em Outcome<br/><b>custo: chamada de rede</b>"]

        retry["<b>Retry / DLQ</b><br/><i>resilience/retry.py</i><br/><br/>backoff com full jitter,<br/>escolha do degrau de TTL"]
    end

    redis[("Redis")]
    pg[("PostgreSQL")]
    sim["Platform<br/>Simulator"]

    broker --> consumer
    consumer --> flags
    flags -->|"protegido"| bulkhead
    bulkhead -->|"slot obtido"| breaker
    breaker -->|"circuito fechado"| limiter
    limiter -->|"ficha disponível"| sender
    sender -->|"HTTP POST"| sim

    bulkhead -.->|"sem slot<br/>BULKHEAD_FULL"| retry
    breaker -.->|"circuito aberto<br/>CIRCUIT_OPEN"| retry
    limiter -.->|"sem ficha<br/>RATE_LIMITED_LOCAL"| retry
    sender -.->|"429 / 5xx / timeout"| retry

    breaker <-->|"Lua atômico"| redis
    limiter <-->|"Lua atômico"| redis
    flags <--> redis
    sender -->|"grava execução"| pg
    retry -->|"republica com atraso"| broker
    retry -.->|"esgotou tentativas"| pg

    classDef comp fill:#85bbf0,stroke:#5d82a8,color:#000
    classDef db fill:#438dd5,stroke:#2e6295,color:#fff
    classDef externo fill:#999999,stroke:#6b6b6b,color:#fff
    class consumer,flags,bulkhead,breaker,limiter,sender,retry comp
    class broker,redis,pg db
    class sim externo
```

### A ordem das camadas não é arbitrária

Cada camada é **mais barata que a seguinte**, e recusar cedo evita gastar o recurso da
próxima:

| # | Camada | Custo | Por que nesta posição |
|---|---|---|---|
| 1 | Feature Flags | cache local | Se a proteção está desligada, nem consultamos o resto |
| 2 | Bulkhead | semáforo em memória | Sem slot de execução, nada mais faz sentido |
| 3 | Circuit Breaker | 1 ida ao Redis | Se a plataforma está fora, **não gaste ficha do bucket** |
| 4 | Rate Limiter | 2 idas ao Redis | A decisão mais cara antes do envio |
| 5 | Envio | chamada de rede | A operação mais cara |

**O detalhe que mais importa nessa ordem:** o breaker vem **antes** do rate limiter.
Consumir uma ficha do bucket para depois descobrir que o circuito está aberto
desperdiçaria cota — e **a ficha não volta**. Invertendo, o circuito filtra primeiro e
o bucket só é tocado quando o envio tem chance real de acontecer.

Dentro do rate limiter, a mesma lógica se repete: o eixo do **conteúdo** (mais
restritivo) é consultado antes do eixo da **plataforma**. Fazer o inverso vazaria
fichas da cota global em requisições que o eixo do conteúdo negaria em seguida.

### Adiamento não é falha

Os quatro caminhos pontilhados para `Retry / DLQ` parecem iguais no diagrama e são
tratados de forma **diferente**:

| Origem | `Outcome` | Conta como? | Contador |
|---|---|---|---|
| Bulkhead cheio | `BULKHEAD_FULL` | adiamento nosso | `defers` |
| Circuito aberto | `CIRCUIT_OPEN` | adiamento nosso | `defers` |
| Sem ficha | `RATE_LIMITED_LOCAL` | adiamento nosso | `defers` |
| 429 / 5xx / timeout | `THROTTLED` / `ERROR` / `TIMEOUT` | **rejeição da plataforma** | `attempt` |

Só as rejeições da plataforma alimentam o circuit breaker e consomem tentativa. Se os
adiamentos contassem, **o rate limiter abriria o circuito ao fazer o seu trabalho** e
tarefas legítimas iriam para a DLQ sem nunca ter sido enviadas. Ver
[ADR-006](adr/ADR-006-circuit-breaker-distribuido.md) e
[ADR-009](adr/ADR-009-retry-com-filas-ttl.md).

---

## Topologia do RabbitMQ

```mermaid
graph LR
    api["API<br/>(dispatcher)"]

    tasks{{"<b>apt.tasks</b><br/><i>topic</i>"}}
    control{{"<b>apt.control</b><br/><i>fanout</i>"}}
    retryex{{"<b>apt.retry</b><br/><i>topic</i>"}}
    dlx{{"<b>apt.dlx</b><br/><i>topic</i>"}}

    qyt["apt.tasks.youtube"]
    qig["apt.tasks.instagram"]
    dlq["<b>apt.dlq</b><br/>sem TTL"]

    r1["apt.retry.1<br/>TTL 1s"]
    r2["apt.retry.2<br/>TTL 5s"]
    r3["apt.retry.3<br/>TTL 30s"]

    cw1["apt.control.worker-1<br/><i>exclusiva</i>"]
    cw2["apt.control.worker-2<br/><i>exclusiva</i>"]

    w1["Worker 1"]
    w2["Worker 2"]

    api -->|"rk: youtube"| tasks
    api -->|"rk: instagram"| tasks
    api --> control

    tasks --> qyt
    tasks --> qig
    control --> cw1
    control --> cw2

    qyt --> w1
    qyt --> w2
    qig --> w1
    qig --> w2
    cw1 --> w1
    cw2 --> w2

    w1 -.->|"rk: tier.N"| retryex
    retryex --> r1
    retryex --> r2
    retryex --> r3

    r1 -.->|"TTL expira → DLX = apt.tasks<br/><b>routing key original preservada</b>"| tasks
    r2 -.-> tasks
    r3 -.-> tasks

    w1 -.->|"esgotou tentativas"| dlx
    qyt -.->|"nack requeue=false"| dlx
    dlx --> dlq

    classDef ex fill:#f9a825,stroke:#c17900,color:#000
    classDef q fill:#85bbf0,stroke:#5d82a8,color:#000
    classDef dead fill:#e57373,stroke:#af4448,color:#000
    class tasks,control,retryex,dlx ex
    class qyt,qig,r1,r2,r3,cw1,cw2 q
    class dlq dead
```

### Três decisões visíveis neste diagrama

**Uma fila por plataforma** — é a camada estrutural do Bulkhead. Mil tarefas de
Instagram acumuladas não ficam à frente das de YouTube, porque estão em outra fila.

**Fanout com fila privada por worker** — com um exchange *topic* e fila compartilhada,
uma invalidação de feature flag chegaria a **um** worker; os outros ficariam com cache
velho. Fanout garante que todos recebem.

**As filas de retry não definem `x-dead-letter-routing-key`** — é isso que faz o
RabbitMQ **preservar a routing key original** (a plataforma) quando o TTL expira e a
mensagem volta. Definir essa chave mandaria todo retry para a fila errada, e o bug
seria silencioso: as mensagens circulariam sem nunca chegar ao consumidor certo.

---

## Fluxo de um envio, de ponta a ponta

```mermaid
sequenceDiagram
    autonumber
    participant A as Admin
    participant API as API + Scheduler
    participant PG as PostgreSQL
    participant MQ as RabbitMQ
    participant W as Worker
    participant R as Redis
    participant P as Plataforma

    A->>API: POST /campaigns
    API->>PG: INSERT campanha + pool de URLs (mesma transação)
    API-->>A: 201 Created

    Note over API: tick do dispatcher (1s)
    API->>PG: SELECT ... FOR UPDATE SKIP LOCKED
    API->>API: plan_tick() → quantas e com que jitter
    API->>PG: UPDATE ... RETURNING (rotação do pool)
    API->>PG: INSERT send_task (grava ANTES de publicar)
    API->>MQ: publish (rk = plataforma)

    MQ->>W: entrega (prefetch=1)
    W->>W: 1. flags (cache local)
    W->>W: 2. bulkhead.acquire()
    W->>R: 3. EVALSHA circuit_breaker.lua (allow)
    R-->>W: allowed=1, state=closed
    W->>R: 4. EVALSHA token_bucket.lua (conteúdo)
    W->>R: 4. EVALSHA token_bucket.lua (plataforma)
    R-->>W: allowed=1, tokens=12.4

    W->>P: 5. POST /engagements
    P-->>W: 200 OK
    W->>R: record_success
    W->>PG: INSERT execution + UPDATE task = sent
    W->>MQ: ack

    Note over W,MQ: caminho alternativo — sem ficha
    R-->>W: allowed=0, retry_after_ms=180
    W->>PG: INSERT execution (RATE_LIMITED_LOCAL)
    W->>MQ: publish apt.retry.1 (defers+1, attempt intacto)
    W->>MQ: ack da original
```

**Observação sobre os passos 4–5.** As duas consultas ao Redis no passo 4 são
`EVALSHA` de scripts Lua, e é aí que a atomicidade acontece. Com
`GET` → decide → `SET`, cinco workers poderiam ler "resta 1 ficha" ao mesmo tempo e
cinco requisições sairiam. Ver
[ADR-005](adr/ADR-005-script-lua-para-atomicidade.md).

**Observação sobre a ordem "grava antes de publicar".** Se a publicação falhar, sobra
uma linha `pending` em `send_tasks` — uma tarefa órfã **visível e auditável**. Na ordem
inversa, o worker receberia uma mensagem cujo `task_id` não existe, e o envio
aconteceria sem registro. Registro sem envio é melhor que envio sem registro.

---

## Documentos relacionados

| Documento | Conteúdo |
|---|---|
| [PADROES.md](PADROES.md) | Os 6 padrões: onde estão no código e como demonstrar |
| [adr/](adr/) | 12 ADRs com as decisões e alternativas rejeitadas |
| [TRADE-OFFS.md](TRADE-OFFS.md) | O custo assumido de cada decisão |
| [CODIGO-EXPLICADO.md](CODIGO-EXPLICADO.md) | Arquivo por arquivo + Q&A antecipado |
| [PLANO-DE-TESTES.md](PLANO-DE-TESTES.md) | Cenários e critérios de aceite |
| [RESULTADOS-TESTES.md](RESULTADOS-TESTES.md) | Números medidos |
| [ATUALIZACOES-DOC-INICIAL.md](ATUALIZACOES-DOC-INICIAL.md) | O que mudou desde o Projeto 02 e por quê |
