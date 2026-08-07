# Anti Platform Throttling

**POC 4**, Projeto Final de **Engenharia de Sistemas Distribuídos** (UFPB, 2026.1)

Sistema distribuído que controla o envio de requisições a plataformas externas, distribuindo a carga ao longo do tempo para não disparar os mecanismos de *rate limiting* e *platform throttling* dessas plataformas.

**Equipe:** Alisson Gabriel de Campos Filho, Antônio Rocha Lima Filho, Cássio Vittori de Campos Filho, João Vitor Teixeira Barreto

***

## A tese do projeto, em um exemplo

O problema não é *ter* um rate limiter. É **onde o estado dele vive**.

```text
Rate limiter EM MEMÓRIA DE PROCESSO (o que quase toda biblioteca oferece):
  1 worker  ×  3 req/s  =   3 req/s enviados   ✓  dentro do limite
  5 workers ×  3 req/s  =  15 req/s enviados   ✗  3× o limite da plataforma

Rate limiter COM ESTADO COMPARTILHADO (este projeto):
  1 worker              →   3 req/s enviados   ✓
  5 workers             →   3 req/s enviados   ✓  o limite é GLOBAL
```

Um rate limiter local passa em qualquer teste com um worker e **falha exatamente ao escalar**, sob carga, quando o limite mais importa, e nunca em desenvolvimento.

O sistema resolve isso com um **token bucket cujo estado vive no Redis**, consultado por um **script Lua atômico**. E o teste `tests/load/scale_test.py` mede exatamente essa propriedade: 1, 3, 5 workers com o pico observado pela plataforma constante.

***

## Padrões arquiteturais

Seis padrões implementados. Detalhes, demonstrações e testes em [docs/PADROES.md](docs/PADROES.md).

| Padrão | Onde vive | Propriedade central |
| :--- | :--- | :--- |
| **Rate Limit / Throttling** | `resilience/rate_limiter.py` + `lua/token_bucket.lua` | Token bucket distribuído, limite **global**, não por processo |
| **Circuit Breaker** | `resilience/circuit_breaker.py` + `lua/circuit_breaker.lua` | Estado compartilhado, um circuito **por plataforma** |
| **Queues / PubSub / Fanout** | `messaging/topology.py` | Topic para tarefas, fanout para controle, DLX/DLQ |
| **Load Balancing** | `messaging/consumer.py` | Competing consumers com `prefetch=1` |
| **Bulkhead / Isolation** | `resilience/bulkhead.py` | Fila, semáforo e pool HTTP **por plataforma** |
| **Feature Flag** | `resilience/feature_flags.py` | Liga e desliga proteções em runtime, propagado por fanout |
| **Retry + DLQ** *(bônus)* | `resilience/retry.py` | Backoff com *full jitter*, o tempo passa **no broker** |

> A escolha de 6 (e não os 7 recomendados para a POC 4) está justificada na ADR 011, conforme exige a Seção 8 do documento da disciplina.

**Áreas técnicas cobertas** (mínimo exigido: 2): Escalabilidade, Desempenho, Confiabilidade e Deployment.

***

## Arquitetura

```text
Admin ──HTTP/REST──▶ API + Scheduler (FastAPI)
                       │  ├─▶ PostgreSQL   campanhas, tarefas, execuções, falhas
                       │  ├─▶ Redis        token buckets, circuito, feature flags
                       │  └─▶ [background task] dispatcher: materializa + jitter
                       ▼
                    RabbitMQ   topic (por plataforma) · fanout (controle)
                       │       DLX/DLQ · 3 filas de retry com TTL
                       ▼
                    Worker × N   flags → bulkhead → breaker → rate limiter → envio
                       ▼
                    Platform Simulator (YouTube · Instagram)
                       └─ janela deslizante real, 429 + Retry-After, injeção de falhas

Prometheus ──scrape──▶ API · Workers (descoberta por DNS) · Simulador
```

Diagramas C4 completos (níveis 1, 2 e 3), topologia do RabbitMQ e diagrama de sequência em [docs/ARQUITETURA.md](docs/ARQUITETURA.md).

### As cinco camadas do worker

A ordem não é arbitrária: cada camada é **mais barata que a seguinte**, e recusar cedo evita gastar o recurso da próxima.

| # | Camada | Custo | Motivo da posição |
| :--- | :--- | :--- | :--- |
| 1 | Feature flags | cache local | Se a proteção está desligada, nem consultamos o resto |
| 2 | Bulkhead | semáforo em memória | Sem slot, nada mais faz sentido |
| 3 | Circuit breaker | 1 ida ao Redis | Se a plataforma está fora, **não gaste ficha** |
| 4 | Rate limiter | 2 idas ao Redis | A decisão mais cara antes do envio |
| 5 | Envio | chamada de rede | A operação mais cara |

***

## Como executar

### Requisitos prévios

*   Docker e Docker Compose
*   Python 3.11+ (apenas para rodar os testes de carga do host)

### Subir o stack

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps            # aguarde todos "healthy"
```

Sete serviços sobem: `postgres`, `redis`, `rabbitmq`, `api`, `worker`, `platform-sim` e `prometheus`.

### Verificar

```bash
curl localhost:8000/health/ready     # {"status":"ready", ...}
curl localhost:9001/admin/stats      # o que as plataformas observaram
```

### Criar uma campanha

```bash
curl -X POST localhost:8000/campaigns \
     -H 'Content-Type: application/json' \
     -d @examples/campaign.json
```

O exemplo pede **300 envios/min (5 req/s)** contra um limite de **3 req/s**, de propósito. Com demanda abaixo do limite, o rate limiter nunca entraria em ação e não haveria nada para observar. Usa `jitter_strategy: "uniform"`, não o `humanized` default da API: `humanized` modula a demanda pela hora do dia, o que tornaria este exemplo mais lento ou mais rápido dependendo de quando o comando é executado.

```bash
curl localhost:8000/campaigns/<id>/status | python -m json.tool
```

O campo `outcome_breakdown` é o que conta a história: `sent` alto, `rate_limited_local` alto (nós nos autolimitamos) e `throttled` **zero** (não fomos bloqueados).

### Interfaces

| O que | URL |
| :--- | :--- |
| OpenAPI / Swagger | http://localhost:8000/docs |
| Prometheus | http://localhost:9090 |
| RabbitMQ (painel) | http://localhost:15672, `apt` / `apt_local_password` |
| Métricas da API | http://localhost:8000/metrics |
| Simulador | http://localhost:9001/docs |

***

## As três demonstrações

### 1. Escalar workers **não** aumenta a vazão

A prova de que o rate limiter é realmente distribuído.

```bash
docker compose up -d --scale worker=5
# rode uma campanha, depois:
curl -s localhost:9001/admin/stats | python -m json.tool
#  ↑ peak_rps continua <= 20, com 1 ou com 5 workers

curl -s localhost:8000/admin/workers | python -m json.tool
#  ↑ carga distribuída entre as réplicas (Load Balancing)
```

### 2. Uma plataforma cai, a outra segue

Circuit breaker e bulkhead.

```bash
curl -X POST localhost:9001/admin/fault -H 'Content-Type: application/json' \
     -d '{"platform":"instagram","mode":"error_500","ttl_seconds":25}'

# o circuito do Instagram abre; o do YouTube nunca abre
curl -s localhost:8000/platforms | python -m json.tool

# a evidência persistida: closed → open → half open → closed
curl -s localhost:8000/admin/breaker-events | python -m json.tool
```

A falha tem TTL: ela expira sozinha e o circuito **se recupera** sem intervenção.

### 3. Desligar o jitter faz os 429 aparecerem

O contrafactual, sem ele, o resultado da primeira demonstração não significaria nada.

```bash
curl -X PATCH localhost:8000/flags/jitter_enabled \
     -H 'Content-Type: application/json' -d '{"value":false}'

sleep 15
curl -s localhost:9001/admin/stats | python -m json.tool
#  ↑ peak_rps sobe e total_throttled deixa de ser zero
```

***

## Testes

```bash
# Qualidade estática, sem infraestrutura
ruff check src tests && ruff format --check src tests && mypy

# 119 testes unitários, sem infraestrutura
pytest tests/unit -v

# Integração, exige o stack no ar
pytest tests/integration -v -m integration
# 54 testes: 44 passed, 1 failed (gap conhecido de fixture, nao bug de
# producao), 9 skipped (por design, quando falta
# infra especifica, nao contam como aprovacao)

# Carga, resiliência e escala, imprimem relatório em markdown
python -m tests.load.load_test
python -m tests.load.resilience_test
python -m tests.load.scale_test
```

**Por que os unitários não precisam de Docker.** A lógica crítica (token bucket, máquina de estados do breaker, jitter, backoff) foi escrita como **função pura**: sem I/O, recebendo o tempo como parâmetro. Isso permite testar exaustivamente os casos de borda, relógio para trás, refill fracionário, transição após cooldown, em milissegundos.

Plano completo com hipóteses e critérios de aceite em [docs/PLANO-DE-TESTES.md](docs/PLANO-DE-TESTES.md). Resultados medidos em [docs/RESULTADOS-TESTES.md](docs/RESULTADOS-TESTES.md).

***

## Documentação

| Documento | Conteúdo |
| :--- | :--- |
| [ARQUITETURA.md](docs/ARQUITETURA.md) | Diagramas C4 (níveis 1, 2 e 3), topologia, sequência |
| [adr/](docs/adr/) | **12 ADRs** com decisões, alternativas rejeitadas e validação |
| [PADROES.md](docs/PADROES.md) | Os 6 padrões: onde no código e como demonstrar |
| [TRADE-OFFS.md](docs/TRADE-OFFS.md) | O custo assumido de cada decisão e as limitações conhecidas |
| [PLANO-DE-TESTES.md](docs/PLANO-DE-TESTES.md) | Cenários, hipóteses e critérios de aceite |
| [RESULTADOS-TESTES.md](docs/RESULTADOS-TESTES.md) | Números medidos e análise |
| [ATUALIZACOES-DOC-INICIAL.md](docs/ATUALIZACOES-DOC-INICIAL.md) | O que mudou desde o Projeto 02 e por quê |

***

## Stack

| Tecnologia | Papel |
| :--- | :--- |
| Python 3.12, FastAPI, uvicorn | API e simulador, assíncronos de ponta a ponta |
| PostgreSQL 16, SQLAlchemy Core, asyncpg | Persistência (SQL explícito, ver ADR 012) |
| Redis 7, scripts Lua | **Estado distribuído**, o núcleo da solução |
| RabbitMQ 3.13, aio-pika | Broker: topic, fanout, DLX/DLQ, retry por TTL |
| httpx | Cliente HTTP com pool por plataforma |
| Prometheus, prometheus-client | 11 métricas instrumentadas |
| structlog | Log estruturado com `correlation_id` |
| Docker, Docker Compose | Orquestração local (7 serviços) |
| GitHub Actions | CI: lint, tipos, testes, build e smoke test |
| pytest, ruff, mypy | Testes e qualidade |

***

## Limitações conhecidas

Declaradas aqui porque um sistema apresentado sem limitações é um sistema mal compreendido. Detalhes em [TRADE-OFFS.md](docs/TRADE-OFFS.md).

1.  **Métricas administrativas sem filtro de campanha.** `/admin/outcomes` e `/admin/workers` somam toda a tabela `executions` sem escopo de tempo ou campanha, em execuções com múltiplas campanhas na mesma base, os números de uma campanha incluem as anteriores. Não afeta `peak_rps` (resetado por cenário), que é a métrica que sustenta a tese central do projeto.
2.  **Os thresholds das plataformas são estimativas**, não números oficiais. A POC valida o **mecanismo** de respeitar um limite desconhecido com margem, não descobre os limites de nenhuma plataforma real. Descobrir esses limites exigiria enviar tráfego artificial a APIs de terceiros, o que viola os termos de uso.
3.  **Falta idempotência ponta a ponta.** A semântica at least once do RabbitMQ permite envio duplicado se um worker morrer entre enviar e dar ack. É a limitação mais séria de projeto ainda em aberto.
4.  **`PATCH /platforms/{platform}` não afeta workers em execução.** Grava no banco, mas os workers leem os parâmetros do `.env`. Declarado no docstring do endpoint e no log.
5.  **Endpoints `/admin/*` sem autenticação.** Aceitável em ambiente local, inaceitável em produção.
6.  **Migrações só na primeira inicialização do volume.** Alterar o schema exige `docker compose down -v`, que apaga os dados.

***

## Ferramentas de IA utilizadas

*Seção obrigatória conforme a Seção 5 do documento da disciplina.*

### Quais ferramentas

*   **Claude (Anthropic)**, via CLI, durante o desenvolvimento.
*   **GitHub Copilot**, autocompletar no editor.

### Em quais partes atuou

**Geração de código:** nenhum módulo foi gerado integralmente por IA. O desenho da arquitetura, a escolha dos algoritmos, a ordem das camadas de proteção e a modelagem do banco foram decididos pela equipe e estão registrados nos ADRs. O uso concentrou-se em **correções e refatorações pontuais**, listadas abaixo.

**Correções pontuais em que a IA ajudou:**

1.  **Atomicidade do consumo de fichas.** A primeira versão do rate limiter fazia `GET`, decide, `SET` em três comandos Redis separados. Ao revisar o trecho, a IA apontou a janela de read modify write entre a leitura e a escrita e sugeriu mover a decisão para um script Lua. A condição de corrida era real e reproduzível, escrevemos `test_concorrencia_nao_estoura_o_limite` para confirmar a falha antes de corrigir.
2.  **Clamp do tempo decorrido no refill.** Perguntamos o que aconteceria se dois workers tivessem relógios levemente defasados. A resposta identificou que um `elapsed` negativo removeria fichas do balde. Adicionamos `max(0, now - updated_at)` e o teste `test_relogio_para_tras_nao_remove_fichas`.
3.  **Separação de `attempt` e `defers`.** Tínhamos um contador único e observamos tarefas indo para a DLQ sem nunca ter sido enviadas. Ao descrever o sintoma, a IA identificou que os adiamentos do rate limiter estavam consumindo tentativas de envio. A correção (dois contadores) foi nossa, o diagnóstico foi conjunto.
4.  **Extração do backoff para função testável.** O cálculo do atraso estava embutido no worker. A IA sugeriu extrair a lógica para `resilience/retry.py` como função pura, o que permitiu testar a **dispersão** do full jitter, a propriedade que de fato importa e que não daria para verificar com a lógica embutida.
5.  **Vazamento de conexão no shutdown do consumer.** A primeira versão fechava a conexão AMQP sem esperar as tarefas em voo. A IA apontou que isso faria o broker reentregar mensagens já enviadas, gerando duplicidade. Daí veio o mecanismo de drenagem com `drain_timeout`.
6.  **Healthchecks e `depends_on` no Compose.** A API subia antes do Postgres aceitar conexão e morria no boot. A IA sugeriu `condition: service_healthy` em vez de `depends_on` simples.
7.  **`ContextVar` em vez de variável de módulo** para o `correlation_id`, a IA apontou que estado global embaralharia ids entre tarefas concorrentes no mesmo worker.

**Revisão:** usamos a IA para revisar os scripts Lua (linguagem que nenhum integrante dominava) e para questionar decisões, foi assim que apareceram as alternativas rejeitadas que hoje estão documentadas nos ADRs (`WATCH`/`MULTI`, Redlock, TTL por mensagem).

**Documentação:** apoio na redação e na estruturação dos ADRs a partir das decisões que a equipe havia tomado, e na revisão de clareza dos comentários de código.

**Testes:** sugestões de casos de borda que não tínhamos considerado, notadamente `test_timeout_nao_vaza_slot` (vazamento de slot do bulkhead após timeout) e os dois casos de **resposta atrasada** no circuit breaker (sucesso e falha de um envio que já estava em voo quando o circuito abriu).

### Como a IA foi orientada

Fornecemos o documento da disciplina, a documentação do Projeto 02 e o contexto do problema. As perguntas foram específicas e sobre trechos concretos, *"esse trecho tem condição de corrida com 5 workers?"*, *"o que acontece se o relógio andar para trás?"*, em vez de pedidos abertos de geração. Sempre que uma sugestão foi aceita, escrevemos primeiro o teste que demonstrava o problema.

### Avaliação honesta

**O que funcionou bem.** Identificar condições de corrida e casos de borda que não tínhamos considerado. Foi consistentemente melhor em *revisar* código que já existia do que em produzir código novo do zero. Também foi útil como interlocutor para argumentar contra decisões, várias alternativas rejeitadas nos ADRs surgiram dessas conversas.

**O que precisou ser corrigido.**

*   Sugeriu inicialmente usar `KEYS apt:rl:*` para limpar os buckets. `KEYS` é bloqueante e percorre todo o keyspace, num Redis com muitas chaves, congelaria o servidor. Trocamos por `SCAN`.
*   Sugeriu `await asyncio.sleep(delay)` para o backoff. Com `prefetch=1`, um worker dormindo segura o seu único slot e para de consumir. Substituímos pelas filas de TTL.
*   Propôs `datetime.utcnow()` em vários pontos, que devolve datetime ingênuo e produz comparações silenciosamente erradas contra `TIMESTAMPTZ`. Criamos `utcnow()` com timezone explícito e proibimos o uso do outro.
*   Em duas ocasiões afirmou limites "reais" de plataformas como se fossem números publicados. Não são verificáveis, e essa é justamente a fronteira que a ADR 008 documenta. Substituímos por estimativas declaradas como tal.

**O que foi descartado.**

*   Sugestão de usar uma biblioteca pronta de circuit breaker (`pybreaker`). Todas mantêm o estado em memória do processo, que é exatamente o problema que o projeto existe para resolver (ADR 006).
*   Sugestão de modelos declarativos do SQLAlchemy ORM. Avaliamos e recusamos: as consultas que importam usam recursos do Postgres que virariam SQL cru de qualquer forma (ADR 012).
*   Sugestão de implementar Traffic Sharding com hashing consistente. Recusada por decisão de escopo da equipe (ADR 011).
*   Sugestões de comentários que apenas repetiam o que o código já dizia.

**Avaliação geral.** A IA acelerou a revisão e ampliou a cobertura de casos de borda. Não substituiu as decisões arquiteturais, e as duas vezes em que aceitamos uma sugestão sem questionar (`KEYS` e `sleep`) produziram justamente os dois problemas que tivemos de desfazer depois.
