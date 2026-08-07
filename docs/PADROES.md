# Padrões arquiteturais aplicados

Seis padrões implementados. Para cada um: **o que é**, **por que aqui**, **onde no
código**, **como demonstrar ao vivo** e **qual teste prova que funciona**.

> A escolha de 6 (e não os 7 recomendados para a POC 4) está justificada em
> [ADR-011](adr/ADR-011-reducao-de-escopo-dos-padroes.md), conforme exige a Seção 8
> do documento da disciplina. O mínimo exigido pela Seção 2.2 é 3.

## Visão geral

| # | Padrão | Área técnica | Arquivo principal | ADR |
|---|---|---|---|---|
| 1 | **Rate Limit / Throttling** | Desempenho | `resilience/rate_limiter.py` | [004](adr/ADR-004-token-bucket-vs-sliding-window.md), [005](adr/ADR-005-script-lua-para-atomicidade.md) |
| 2 | **Circuit Breaker** | Confiabilidade | `resilience/circuit_breaker.py` | [006](adr/ADR-006-circuit-breaker-distribuido.md) |
| 3 | **Queues / PubSub / Fanout** | Escalabilidade | `messaging/topology.py` | [001](adr/ADR-001-rabbitmq-como-broker.md) |
| 4 | **Load Balancing** | Escalabilidade | `messaging/consumer.py` | [001](adr/ADR-001-rabbitmq-como-broker.md) |
| 5 | **Bulkhead / Isolation** | Confiabilidade | `resilience/bulkhead.py` | [007](adr/ADR-007-bulkhead-com-filas-dedicadas.md) |
| 6 | **Feature Flag** | Deployment | `resilience/feature_flags.py` | [003](adr/ADR-003-redis-para-estado-distribuido.md) |
| + | **Retry + DLQ** *(bônus)* | Confiabilidade | `resilience/retry.py` | [009](adr/ADR-009-retry-com-filas-ttl.md) |

**Áreas técnicas cobertas (mínimo exigido: 2):** Escalabilidade, Desempenho,
Confiabilidade e Deployment.

---

## 1. Rate Limit / Throttling

### O que é

Controlar a quantidade de requisições emitidas num intervalo de tempo. Implementado
como **token bucket**: um balde com `capacity` fichas, reposto a `refill_rps`
fichas por segundo. Cada envio consome uma ficha; sem ficha, o envio é adiado e o
chamador recebe **quanto tempo falta** para a próxima.

### Por que aqui

É o padrão central da POC — o problema que dá nome ao projeto. Mas o detalhe que
importa não é *que* existe um rate limiter, e sim **onde o estado dele vive**:

```
Estado EM MEMÓRIA de cada processo:
  5 workers ×  3 req/s = 15 req/s enviados   ← 3× o limite da plataforma

Estado COMPARTILHADO no Redis:
  5 workers            =  3 req/s enviados   ← o limite é GLOBAL
```

Um rate limiter local passaria em qualquer teste com um worker e **falharia
exatamente ao escalar**.

### Onde no código

| Arquivo | Papel |
|---|---|
| `src/apt/resilience/token_bucket.py` | Algoritmo puro — implementação de **referência**, testável sem infraestrutura |
| `src/apt/resilience/lua/token_bucket.lua` | Execução **atômica** dentro do Redis |
| `src/apt/resilience/rate_limiter.py` | Fachada com os dois eixos de limitação |

**Dois eixos, e a ordem entre eles importa.** Cada envio consulta o balde do
**conteúdo** (4 req/s por URL) e depois o da **plataforma** (3 req/s). O eixo do
conteúdo vem primeiro porque negar ali evita gastar uma ficha da cota global numa
requisição que não vai sair — e a ficha não volta.

O eixo por conteúdo existe porque concentrar volume numa única URL é o padrão que os
sistemas de detecção procuram. Limitar apenas a plataforma permitiria 3 req/s numa
URL só: dentro do limite agregado, e obviamente artificial.

### Como demonstrar

```bash
# Demanda de 10 req/s contra um limite de 3 req/s.
# jitter_strategy=uniform, nao humanized: humanized modula a demanda pela hora
# do dia e tornaria este numero dependente de quando o comando roda.
curl -X POST localhost:8000/campaigns -H 'Content-Type: application/json' -d '{
  "name":"Demo rate limit","platform":"youtube","total_sends":150,
  "target_rate_per_min":600,"jitter_strategy":"uniform",
  "contents":[{"url":"https://yt/a"},{"url":"https://yt/b"},{"url":"https://yt/c"}]}'

sleep 20
curl -s localhost:9001/admin/stats | python -m json.tool
#  ↑ peak_rps deve ficar <= 5 (o limite da plataforma) e total_throttled = 0

curl -s localhost:8000/admin/outcomes | python -m json.tool
#  ↑ "sent" alto, "rate_limited_local" alto, "throttled" = 0
#    Traduzindo: nós nos autolimitamos N vezes e fomos bloqueados 0 vezes.
```

### Testes que provam

- `tests/unit/test_token_bucket.py` — 17 casos: bucket vazio, refill fracionário,
  relógio para trás, pedido maior que a capacidade, e a garantia de que **negar não
  consome crédito**.
- `tests/integration/test_rate_limiter_redis.py::TestConcorrencia` — 50 corrotinas
  simultâneas, exatamente `capacity` passam.
- `tests/integration/test_rate_limiter_redis.py::TestParidade` — Lua × Python, mesma
  sequência, mesmos resultados.
- **`tests/load/scale_test.py`** — 1 → 3 → 5 workers com pico constante. A validação
  de ponta a ponta.

---

## 2. Circuit Breaker

### O que é

Um interruptor que **para de tentar** quando um serviço está falhando. Três estados:

```
CLOSED  ──── N falhas consecutivas ────▶  OPEN
   ▲                                        │
   │                              passou o cooldown
   │                                        ▼
   └──── M sucessos consecutivos ────  HALF_OPEN
                                            │
              qualquer falha ───────────────┘ (reabre)
```

### Por que aqui

Insistir contra uma plataforma em problema (a) não vai funcionar, (b) consome nossos
recursos em timeouts, (c) piora a situação dela e (d) — o mais importante neste
domínio — **prolonga a punição**: muitas plataformas renovam a janela de bloqueio a
cada nova tentativa recebida durante a penalidade.

**O estado é compartilhado**, e isso muda o comportamento qualitativamente. Com
breaker por processo e threshold 5, cinco workers precisariam de 25 requisições
falhas antes do primeiro circuito abrir — e os outros quatro continuariam martelando.
Aqui, a quinta falha vista por **qualquer** worker abre o circuito para **todos**.

### Onde no código

| Arquivo | Papel |
|---|---|
| `src/apt/resilience/breaker_state.py` | Máquina de estados pura — implementação de referência |
| `src/apt/resilience/lua/circuit_breaker.lua` | Transições atômicas no Redis |
| `src/apt/resilience/circuit_breaker.py` | Fachada + persistência das transições |

**Um circuito por plataforma**, não um global — é a junção com o Bulkhead.

**Somente rejeições da plataforma contam como falha.** 429, 5xx e timeout contam;
adiamentos internos (rate limiter, bulkhead) **não**. Se contassem, o rate limiter
funcionando corretamente abriria o circuito.

### Como demonstrar

```bash
# Derruba o Instagram
curl -X POST localhost:9001/admin/fault -H 'Content-Type: application/json' \
     -d '{"platform":"instagram","mode":"error_500","ttl_seconds":25}'

# Acompanha o circuito abrindo e depois recuperando sozinho
watch -n 2 'curl -s localhost:8000/platforms | python -m json.tool | grep -E "platform|circuit"'

# A evidência persistida
curl -s localhost:8000/admin/breaker-events | python -m json.tool
#  ↑ closed → open  ... e depois  open → half_open → closed
```

### Testes que provam

- `tests/unit/test_breaker_state.py` — 11 casos, incluindo sucesso zerando o contador,
  falha em `half_open` reabrindo, e os dois casos de resposta atrasada.
- **`tests/integration/test_circuit_breaker_redis.py::TestEstadoCompartilhado`** —
  cinco processos simulados, uma falha cada, e um sexto que nunca viu falha já
  encontra o circuito aberto.
- `tests/load/resilience_test.py` (hipótese H1) — ciclo completo de ponta a ponta.

---

## 3. Queues / PubSub / Fanout (+ DLQ)

### O que é

Comunicação assíncrona mediada por um broker. Três formas de roteamento no projeto:

| Exchange | Tipo | Uso |
|---|---|---|
| `apt.tasks` | **topic** | roteia por plataforma → uma fila cada |
| `apt.control` | **fanout** | eventos de controle → **todos** os workers |
| `apt.retry` | topic | três filas com TTL para o backoff |
| `apt.dlx` | topic | falhas terminais → `apt.dlq` |

### Por que aqui

O ritmo de entrada é diferente do de saída: o administrador cria 10.000 envios num
POST; as plataformas aceitam poucas requisições por segundo (3 a 10, dependendo da
plataforma). Sem fila, a requisição HTTP ficaria presa por dezenas de minutos. Com
fila, a API responde em milissegundos e a fila absorve o pico.

**Por que fanout para o controle.** Uma invalidação de feature flag precisa chegar a
**todas** as réplicas. Com exchange *topic* e fila compartilhada, o RabbitMQ entregaria
a mensagem a **um** worker e os outros ficariam com cache velho.

### Onde no código

`src/apt/messaging/topology.py` — a definição única da topologia, com o diagrama no
próprio docstring. `publisher.py` publica com *publisher confirms* e mensagem
persistente; `consumer.py` consome com ack manual.

### Como demonstrar

```bash
# Painel do RabbitMQ: apt.tasks.youtube, apt.tasks.instagram, apt.retry.*, apt.dlq
open http://localhost:15672   # usuário/senha: apt / apt_local_password

# Evidência que funciona de ponta a ponta (padrão Circuit Breaker, Demo 2):
# closed -> open -> half_open -> closed
curl -s localhost:8000/admin/breaker-events | python -m json.tool
```

`/admin/failures` (o que foi para a DLQ) foi retirado do roteiro de demonstração: o bug
de retry documentado em [TRADE-OFFS.md](TRADE-OFFS.md) (item 14) faz tarefas adiadas
desaparecerem antes de completar as tentativas e chegar à DLQ, então a tabela `failures`
não é uma evidência confiável para uma demo ao vivo enquanto esse bug não for corrigido.

### Testes que provam

- `tests/integration/test_messaging.py::TestRetryComTTL::test_retry_volta_para_a_fila_original`
- **`TestFanoutDeControle::test_todas_as_filas_recebem_o_evento`** — duas filas
  recebem a mesma mensagem. Com topic, apenas uma receberia.

---

## 4. Load Balancing

### O que é

Distribuir trabalho entre várias instâncias. Aqui, via **competing consumers**: N
workers consomem a mesma fila e o broker entrega cada mensagem a um deles.

### Por que aqui

É o que faz `--scale worker=5` funcionar sem nenhuma mudança de código e sem
coordenação entre as réplicas.

### Onde no código

`src/apt/messaging/consumer.py`, e a decisão cabe numa linha:

```python
await channel.set_qos(prefetch_count=settings.worker_prefetch)  # = 1
```

**Por que `prefetch=1`.** O padrão do AMQP é ilimitado. Com prefetch alto, o primeiro
worker a conectar puxa **todas** as mensagens disponíveis para o buffer local e as
processa em série — enquanto as outras réplicas ficam paradas, com a fila vazia. A
fila parece equilibrada no painel, mas não está: as mensagens estão empilhadas na
memória de um worker só.

### Como demonstrar

```bash
docker compose up -d --scale worker=5
# ... roda uma campanha ...
curl -s localhost:8000/admin/workers | python -m json.tool
#  ↑ distribuição aproximadamente uniforme entre as réplicas
```

### Testes que provam

`tests/load/scale_test.py` mede a razão entre a réplica mais e a menos usada
(`distribution_ratio`) e exige que pelo menos 2 réplicas recebam trabalho.

---

## 5. Bulkhead / Isolation

### O que é

Compartimentos estanques. O nome vem da engenharia naval: um navio dividido em
compartimentos não afunda quando um deles inunda.

### Por que aqui

O cenário **sem** bulkhead: o Instagram passa a responder em 5 segundos. Cada envio
ocupa uma corrotina por 5 segundos. Como os envios das duas plataformas compartilham
o pool de execução, em poucos segundos todos os slots estão presos — e os envios de
YouTube ficam na fila atrás deles.

O resultado é o pior tipo de falha: **silenciosa**. Nenhum erro aparece; a vazão do
YouTube simplesmente despenca, por um motivo que não tem nada a ver com o YouTube.

### Onde no código — três camadas

| Camada | Recurso isolado | Arquivo |
|---|---|---|
| Fila dedicada | posição na fila (broker) | `messaging/topology.py` |
| Semáforo | slots de execução (worker) | `resilience/bulkhead.py` |
| Pool HTTP | conexões de rede | `worker/sender.py` |

As três são necessárias — cada uma sozinha deixa um recurso compartilhado.

**Fail-fast, não espera.** Sem slot em 2s, o envio é recusado e a tarefa volta à fila.
Espera sem limite transformaria o semáforo numa fila invisível: as tarefas não
apareceriam em lugar nenhum e a latência medida perderia significado.

### Como demonstrar

```bash
curl -X POST localhost:9001/admin/fault -H 'Content-Type: application/json' \
     -d '{"platform":"instagram","mode":"timeout","ttl_seconds":30}'

# O YouTube continua enviando na vazão normal enquanto o Instagram está travado
watch -n 2 'curl -s localhost:9001/admin/stats | python -m json.tool | grep -E "platform|accepted"'
```

### Testes que provam

- **`tests/unit/test_bulkhead.py::TestIsolamento::test_plataforma_esgotada_nao_afeta_a_outra`**
- **`test_timeout_nao_vaza_slot`** — após 5 timeouts, a capacidade original continua
  disponível. Guarda o modo de falha mais perigoso do padrão.
- `tests/load/resilience_test.py` (H2) — mede envios de YouTube **durante** a falha do
  Instagram.

---

## 6. Feature Flag

### O que é

Alterar comportamento em runtime, sem redeploy. Estado no Redis, cache local de 2s,
invalidação ativa por fanout.

| Flag | Efeito |
|---|---|
| `rate_limiter_enabled` | desliga o rate limiter |
| `circuit_breaker_enabled` | desliga o circuit breaker |
| `jitter_enabled` | desliga a distribuição temporal (envios em rajada) |
| `auto_pause_on_open` | pausa campanhas quando um circuito abre |
| `dispatch_enabled` | para de materializar novas tarefas |

### Por que aqui

Além do papel operacional clássico, as flags têm uma função específica nesta POC: o
rate limiter e o circuit breaker são mecanismos que **funcionam invisivelmente quando
estão certos**. As flags permitem desligá-los na mesma execução e mostrar o
contrafactual.

É o que transforma a apresentação de *"confie em nós, está funcionando"* em *"olhe os
números antes e depois"*.

### Onde no código

`src/apt/resilience/feature_flags.py` (cache + leitura) e
`src/apt/api/routers/flags.py` (escrita + publicação do evento de fanout).

Todas as proteções começam **ligadas**: se o Redis estiver vazio ou inacessível, o
sistema opera protegido. Uma flag ausente nunca significa "desligue a proteção".

### Como demonstrar

```bash
# Desliga o jitter: os envios passam de distribuídos para rajada
curl -X PATCH localhost:8000/flags/jitter_enabled \
     -H 'Content-Type: application/json' -d '{"value":false}'

sleep 15
curl -s localhost:9001/admin/stats | python -m json.tool
#  ↑ peak_rps sobe e total_throttled deixa de ser zero

curl -X PATCH localhost:8000/flags/jitter_enabled \
     -H 'Content-Type: application/json' -d '{"value":true}'
```

### Testes que provam

- `tests/integration/test_messaging.py::TestFanoutDeControle` — a propagação.
- `tests/load/load_test.py` — usa `rate_limiter_enabled=false` para produzir o
  cenário contrafactual. **Sem ele, o resultado do cenário protegido não
  significaria nada:** não daria para saber se os 429 não apareceram por causa do
  rate limiter ou porque a carga era baixa.

---

## Bônus — Retry Pattern + DLQ

Não estava entre os recomendados para a POC 4, mas consta na lista de Confiabilidade
(Seção 6.4) e completa o padrão de filas.

**Backoff exponencial com full jitter**, três degraus de TTL no broker, e **dois
contadores separados**: `attempt` (falhas de envio) e `defers` (adiamentos nossos).

A separação dos contadores corrige um bug conceitual real: com um contador único, uma
tarefa adiada 4 vezes pelo rate limiter iria para a DLQ **sem nunca ter sido
enviada** — o sistema descartaria trabalho legítimo justamente quando estivesse se
protegendo corretamente.

O tempo do backoff passa **dentro do broker**, não em `sleep()` no worker. Com
`prefetch=1`, um worker dormindo 30s segura o seu único slot e para de consumir; cinco
workers em backoff longo travariam o sistema.

Detalhes e alternativas rejeitadas em
[ADR-009](adr/ADR-009-retry-com-filas-ttl.md).

---

## Como os padrões se compõem

Nenhum deles resolve o problema sozinho. A composição é o que funciona:

```
Feature Flag         decide SE as proteções estão ativas
       │
Bulkhead             garante que há recurso local para tentar
       │
Circuit Breaker      evita tentar contra plataforma que está fora
       │
Rate Limiter         garante que a vazão global respeita o limite
       │
    ENVIO
       │
Retry + DLQ          o que falhou volta com backoff; o que não tem
                     salvação fica auditável
       │
Queues + Load Bal.   distribuem tudo isso entre N réplicas
```

E há uma interação que merece destaque, porque é onde a implementação ingênua erra:
**o rate limiter e o circuit breaker precisam de contadores separados**. Se o
adiamento do primeiro alimentasse o segundo, o sistema se autobloquearia ao se
proteger. É por isso que `Outcome` separa `is_platform_rejection` de
`is_self_throttled`, e há um teste garantindo que os dois grupos são disjuntos.
