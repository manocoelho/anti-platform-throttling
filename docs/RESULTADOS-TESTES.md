# Resultados dos testes

Este documento tem **duas partes**, separadas de propósito:

- **Parte 1 — Resultados medidos.** Números reais, coletados e reproduzíveis.
- **Parte 2 — Pendente de execução.** Os cenários que exigem o stack no ar e **não foram
  executados** na máquina em que o sistema foi desenvolvido, com a razão técnica e as
  instruções para completá-los.

A separação existe porque relatar como medido algo que não foi medido é pior que declarar
a lacuna. A dimensão "Testes e Validação" avalia o plano, a execução e as métricas — e um
número inventado invalida as três.

Plano completo com hipóteses e critérios de aceite em
[PLANO-DE-TESTES.md](PLANO-DE-TESTES.md).

---

# Parte 1 — Resultados medidos

## 1.1 Qualidade estática

Ambiente: Python 3.13.7, Windows 11.

```
$ ruff check src tests
All checks passed!

$ ruff format --check src tests
64 files already formatted

$ mypy
Success: no issues found in 44 source files
```

| Verificação | Ferramenta | Resultado |
|---|---|---|
| Lint (E, F, I, UP, B, ASYNC, C4, SIM, RUF) | ruff 0.8+ | **0 problemas** |
| Formatação | ruff format | **64 arquivos conformes** |
| Tipagem estática (`disallow_untyped_defs`) | mypy 1.13+ | **0 erros em 44 arquivos** |

A configuração do mypy é estrita: `disallow_untyped_defs = true` exige anotação em toda
função. Não há `# type: ignore` sem código de erro específico.

## 1.2 Testes unitários

```
$ pytest tests/unit -q
.........................................................................
.............................................
117 passed in 2.09s
```

| Arquivo | Testes | O que cobre |
|---|---|---|
| `test_retry.py` | 28 | Backoff com full jitter, escolha de degrau, status retentáveis |
| `test_domain.py` | 20 | Contrato da mensagem, `Outcome`, invariantes dos perfis |
| `test_jitter.py` | 18 | Distribuição temporal, perfil diário, convergência de taxa fracionária |
| `test_token_bucket.py` | 17 | Casos de borda do rate limiter |
| `test_platform_sim.py` | 12 | Janela deslizante, `peak_rps`, expiração de falha |
| `test_breaker_state.py` | 11 | Transições do circuito, respostas atrasadas |
| `test_bulkhead.py` | 11 | Isolamento entre plataformas, não vazamento de slot |
| **Total** | **117** | **2.09 s, sem nenhuma infraestrutura** |

### Os testes mais lentos

```
0.19s  test_bulkhead.py::TestBulkhead::test_timeout_nao_vaza_slot
0.11s  test_bulkhead.py::TestIsolamento::test_carga_concorrente_respeita_a_capacidade
0.05s  test_bulkhead.py::TestBulkhead::test_recusa_quando_cheio_apos_o_timeout
0.03s  test_bulkhead.py::TestIsolamento::test_plataforma_esgotada_nao_afeta_a_outra
0.01s  test_retry.py::TestBackoff::test_teto_cresce_exponencialmente
```

Os quatro mais lentos são do bulkhead, e por um motivo legítimo: eles **precisam** esperar
o timeout real do semáforo (20–50 ms) para verificar o comportamento de fail-fast. Somados,
custam 0.38 s.

Os demais 113 testes rodam em ~1.7 s **porque a lógica que eles verificam é pura**: token
bucket, máquina de estados do breaker, jitter e backoff não fazem I/O e recebem o tempo
como parâmetro. É o que permite testar `OPEN → HALF_OPEN após 15 segundos` sem esperar 15
segundos.

## 1.3 Cobertura

```
$ pytest tests/unit --cov=apt --cov-report=term
```

**A leitura correta desta tabela não é o total (40%), e sim a distribuição.**

### Módulos de lógica pura — o núcleo da POC

| Módulo | Statements | Cobertura |
|---|---|---|
| `resilience/token_bucket.py` | 31 | **100%** |
| `resilience/retry.py` | 25 | **100%** |
| `platform_sim/throttle.py` | 55 | **100%** |
| `resilience/breaker_state.py` | 55 | **98%** |
| `scheduling/jitter.py` | 60 | **98%** |
| `domain/models.py` | 99 | **98%** |
| `domain/platforms.py` | 22 | **95%** |
| `observability/metrics.py` | 23 | **91%** |
| `resilience/bulkhead.py` | 58 | **86%** |
| `config.py` | 68 | **81%** |

**Os mecanismos que sustentam a tese do projeto estão entre 86% e 100%** — e isso foi
possível justamente porque a decisão de escrevê-los como funções puras (ADR-004, ADR-005)
os tornou testáveis sem infraestrutura.

### Módulos de I/O — cobertos pelos testes de integração

| Módulo | Cobertura unitária | Coberto por |
|---|---|---|
| `resilience/rate_limiter.py` | 40% | `test_rate_limiter_redis.py` |
| `resilience/circuit_breaker.py` | 33% | `test_circuit_breaker_redis.py` |
| `resilience/feature_flags.py` | 44% | `test_messaging.py` (fanout) |
| `messaging/topology.py` | 49% | `test_messaging.py` |
| `messaging/publisher.py` | 38% | `test_messaging.py` |
| `messaging/consumer.py` | 27% | `test_messaging.py` |
| `db/repositories.py` | 53% | `test_api_campaigns.py` |
| `scheduling/dispatcher.py` | 29% | smoke test do CI |
| `worker/main.py` | 0% | smoke test do CI |
| `api/*` | 0% | `test_api_campaigns.py` |
| **TOTAL** | **40%** | |

Estes módulos são **fachadas** e **orquestração**: traduzem chamadas para Redis, RabbitMQ,
Postgres e HTTP. Testá-los sem a infraestrutura real exigiria mocks tão detalhados que o
teste passaria a verificar o mock, não o comportamento. É por isso que existem os testes de
integração — e é por isso que a cobertura de 40% aqui **não** significa 40% do sistema
verificado: significa que 60% dele é verificado por outra camada da suíte.

`worker/main.py` (226 statements, 0%) é o caso mais visível: ele é o orquestrador das cinco
camadas, e cada camada individual está coberta. O que falta é o teste da **composição**, e
esse é o smoke test do CI.

## 1.4 Estrutura do repositório

| Métrica | Valor |
|---|---|
| Arquivos Python de produção | 44 |
| Statements de produção | 2.176 |
| Arquivos Python de teste | 20 |
| Testes unitários | 117 |
| Testes de integração | 54 (em 4 arquivos) |
| Cenários de carga/resiliência/escala | 3 |
| Scripts Lua | 2 |
| ADRs | 12 (+ índice) |
| Documentos de arquitetura/processo | 8 |
| Serviços no Docker Compose | 7 |
| Rotas na API | 21 |
| Métricas Prometheus | 11 |

---

# Parte 2 — Pendente de execução

## 2.1 O que não foi executado, e por quê

Os testes de **integração** e os três cenários de **carga, resiliência e escala** exigem
Postgres, Redis e RabbitMQ no ar. Eles **não foram executados** na máquina de
desenvolvimento.

**Razão técnica.** O runtime de containers do Docker Desktop nesta máquina **cria**
containers mas não os **inicia**. O diagnóstico:

```
$ docker version --format '{{.Server.Version}}'
29.6.2                                    ← o daemon responde

$ docker pull python:3.12-slim
Status: Downloaded newer image             ← o daemon baixa imagens

$ timeout 45 docker run --rm alpine echo "teste"
(nenhuma saída — o timeout encerra)        ← o container NÃO executa

$ docker ps -a --filter status=created
affectionate_varahamihira | alpine:latest  ← fica preso em "Created"
gracious_zhukovsky        | python:3.12-slim
```

O sintoma se manifestou primeiro durante o `docker compose build`: o passo
`RUN apt-get update` ficou parado indefinidamente, e depois o `RUN pip install` também.
Nenhum dos dois é um problema do projeto — nenhum comando `RUN` executa, porque nenhum
container inicia.

Ambiente: Windows 11 Pro 10.0.26100, Docker Desktop 29.6.2, backend WSL2
(kernel 6.6.87.2-microsoft-standard-WSL2). Tanto `Ubuntu` quanto `docker-desktop`
aparecem como `Running` no `wsl --list --verbose`.

**Duas melhorias reais saíram desse diagnóstico** e estão no código:

1. **O `Dockerfile` não tem mais `apt-get`.** `gcc`/`libc6-dev` eram desnecessários (o
   `asyncpg` publica wheels manylinux para CPython 3.12) e `curl` foi substituído por
   `python -c` com `urllib.request` nos healthchecks. A imagem final passa a não ter
   gerenciador de pacotes nem compilador — menos superfície de vulnerabilidade — e o build
   deixa de depender da disponibilidade dos repositórios Debian.
2. **Os três serviços Python compartilham `image: apt-app:local`.** Sem o `image:`
   explícito, o Compose construía a **mesma** imagem **três vezes**.

## 2.2 Como completar esta seção

Numa máquina com Docker funcional:

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps                              # aguarde todos "healthy"

pytest tests/integration -v -m integration     # 54 testes
python -m tests.load.load_test                 # cenário A
python -m tests.load.resilience_test           # cenário B
python -m tests.load.scale_test                # cenário C
```

Cada um dos três cenários **imprime o relatório em markdown já formatado**, pronto para
substituir a seção correspondente abaixo. Os scripts também avaliam os critérios de aceite
e retornam exit code ≠ 0 quando algum falha — não é necessário interpretar os números na
mão.

## 2.3 Cenário A — Carga

> **NÃO EXECUTADO.** Substituir por `python -m tests.load.load_test`.

**Hipótese:** sob demanda de 30 req/s contra limite de 16 req/s, o sistema não recebe 429 e
registra o excedente como adiamento.

| Métrica | Proteções ligadas | Rate limiter desligado |
|---|---|---|
| Envios aceitos (2xx) | _pendente_ | _pendente_ |
| **429 recebidos da plataforma** | _pendente_ | _pendente_ |
| Adiados por nós (`rate_limited_local`) | _pendente_ | _pendente_ |
| Pico observado pela plataforma (req/s) | _pendente_ | _pendente_ |
| Limite da plataforma (req/s) | 20 | 20 |

**Latência (cenário protegido):** p50 / p95 / p99 — _pendente_

| Critério de aceite | Esperado |
|---|---|
| A-1 · com proteção, 429 recebidos | `= 0` |
| A-2 · pico observado ≤ limite da plataforma | `≤ 20 req/s` |
| A-3 · excedente foi adiado, não descartado | `rate_limited_local > 0` |
| A-4 · **contrafactual:** sem proteção os 429 aparecem | `throttled(A2) > throttled(A1)` |

## 2.4 Cenário B — Resiliência

> **NÃO EXECUTADO.** Substituir por `python -m tests.load.resilience_test`.

**Hipóteses:** H1 — o circuito do Instagram abre, sonda e fecha sozinho. H2 — o YouTube
continua enviando durante a falha do Instagram.

**Linha do tempo** (amostragem a cada 3 s) — _pendente_

**Transições registradas em `breaker_events`** — _pendente_

| Critério de aceite | Esperado |
|---|---|
| B-1 · o circuito do Instagram abriu | `open` presente |
| B-2 · o circuito sondou | `half_open` em `breaker_events` |
| B-3 · o circuito fechou após a falha expirar | estado final `closed` |
| B-4 · o circuito do YouTube nunca abriu | `open` ausente |
| B-5 · o YouTube seguiu enviando durante a falha | envios aceitos `> 0` |

## 2.5 Cenário C — Escala

> **NÃO EXECUTADO.** Substituir por `python -m tests.load.scale_test`.
>
> **É o teste mais importante do projeto** — a prova de que o rate limiter é distribuído.

| Workers | Aceitos | **429** | Adiados | **Pico observado pela plataforma** | Duração | p95 |
|---|---|---|---|---|---|---|
| 1 | _pendente_ | _pendente_ | _pendente_ | _pendente_ | _pendente_ | _pendente_ |
| 3 | _pendente_ | _pendente_ | _pendente_ | _pendente_ | _pendente_ | _pendente_ |
| 5 | _pendente_ | _pendente_ | _pendente_ | _pendente_ | _pendente_ | _pendente_ |

| Critério de aceite | Esperado |
|---|---|
| C-1 · pico ≤ limite da plataforma em **todas** as configurações | `≤ 20 req/s` |
| C-2 · zero 429 em **todas** | `= 0` |
| C-3 · **o pico NÃO cresce com o número de workers** | variação `≤ 1.25×` |
| C-4 · load balancing entre réplicas | `≥ 2` réplicas, razão `≤ 4.0` |

**C-3 é a hipótese central.** Com um rate limiter em memória de processo, o pico cresceria
proporcionalmente ao número de workers (~5× com 5 workers). Se a medição mostrar
crescimento linear, a tese do projeto está errada — e o número entra no relatório como
está, com análise.

## 2.6 Consultas PromQL para a demonstração

Prontas para colar em http://localhost:9090 durante a apresentação.

**Vazão efetiva por plataforma** — deve ficar abaixo do `allowed_rps`:

```promql
sum by (platform) (rate(apt_sends_total{outcome="sent"}[30s]))
```

**A prova visual do cenário C** — a linha acima **não muda** quando as réplicas sobem de 1
para 5. Compare com o número de réplicas:

```promql
count(up{job="apt-worker"})
```

**429 recebidos** — a linha que deve permanecer em zero:

```promql
sum by (platform) (rate(apt_sends_total{outcome="throttled"}[30s]))
```

**Autolimitação × bloqueio** — as duas séries juntas contam a história da POC:

```promql
sum by (outcome) (rate(apt_sends_total{outcome=~"sent|rate_limited_local|throttled"}[30s]))
```

**Estado dos circuitos** (0 = closed, 1 = half_open, 2 = open):

```promql
apt_circuit_state
```

**Fichas disponíveis no balde** — cai a zero sob carga e recupera:

```promql
apt_rate_limit_tokens
```

**Latência p95 por plataforma:**

```promql
histogram_quantile(0.95, sum by (platform, le) (rate(apt_send_latency_seconds_bucket[1m])))
```

**Atraso introduzido pelo rate limiter** — quanto ele está atrasando os envios para manter
a vazão dentro do limite:

```promql
histogram_quantile(0.95, sum by (platform, le) (rate(apt_schedule_delay_seconds_bucket[1m])))
```

**Ocupação dos compartimentos do bulkhead:**

```promql
apt_bulkhead_in_use
```

**Rejeições do bulkhead** (fail-fast em ação):

```promql
sum by (platform) (rate(apt_bulkhead_rejections_total[1m]))
```

**Pico observado pela plataforma, do lado dela:**

```promql
sum by (platform, status) (rate(apt_sim_requests_total[30s]))
```

---

## Nota de método

Duas coisas que valem ser ditas na apresentação sobre esta suíte.

**O contrafactual não é opcional.** O cenário A roda **duas vezes**: com e sem o rate
limiter. Sem a segunda execução, "zero 429" não significaria nada — não daria para
distinguir "o rate limiter funcionou" de "a carga era baixa". É o mesmo raciocínio de um
grupo de controle.

**A evidência mais forte vem do lado de fora.** O `peak_rps` reportado por
`GET /admin/stats` do simulador é o pico que a **plataforma** observou, medido por ela. Um
sistema pode registrar internamente o que quiser; o que importa é o que chegou do outro
lado da fronteira.
