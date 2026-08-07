# Resultados dos testes

Números da execução **mais recente**, depois de quatro rodadas: a primeira encontrou
três bugs reais; a segunda corrigiu dois deles e, ao validar a segunda, encontrou um
quarto; a terceira corrigiu o quinto (perda de mensagens no retry — a routing key do
dead-letter) e dois defeitos no **critério de aceite** e no **desenho do
contrafactual** do próprio plano de testes; esta quarta rodada recalibrou o YouTube
(e só o YouTube) porque o teto de vazão do próprio ambiente de teste tinha virado um
confundidor da hipótese central, descobriu — ao medir, não por inspeção — um segundo
confundidor independente no desenho dos scripts de carga, e corrigiu o critério do C-3
uma segunda vez, porque a correção da rodada anterior contradizia uma invariante que o
projeto já declarava em outro lugar. Os números de rodadas anteriores não aparecem
lado a lado com os atuais — ver § 1.0 para o que cada uma encontrou, sem repetir os
números delas aqui.

Plano completo com hipóteses e critérios de aceite em
[PLANO-DE-TESTES.md](PLANO-DE-TESTES.md). Causa raiz completa de cada bug, evidência de
reprodução e a correção (aplicada ou proposta) em [TRADE-OFFS.md](TRADE-OFFS.md), itens
14–18.

---

## 1.0 O que cada rodada encontrou

**Primeira execução** (sem nenhuma correção, testes de integração e carga rodando pela
primeira vez contra infraestrutura real): três bugs reais.

1. **Calibração do burst insuficiente** — `burst_capacity + allowed_rps` podia superar
   o limite da plataforma por construção. **Corrigido** na segunda rodada (item 16).
2. **Vazamento de sonda no `half_open` do circuit breaker** — uma sonda admitida e depois
   adiada por outra camada nunca liberava o slot; o circuito travava em `half_open` para
   sempre. **Corrigido** na segunda rodada (item 15).
3. **Perda de mensagens no retry por routing key incorreta** — encontrado, mas avaliado
   como de baixo impacto imediato (o bug do item 2 mascarava o caminho de adiamento que
   este bug destrói) e não corrigido naquela rodada.

**Segunda rodada** (aplicando as correções 1 e 2 acima): ao validar a correção do
vazamento de sonda, uma **quarta causa raiz** apareceu — o dispatcher publicava cada
mensagem no RabbitMQ **dentro** da transação que materializa o tick inteiro, antes dela
comitar. Um worker local rápido podia consumir a mensagem antes de a linha em
`send_tasks` estar visível para a sua própria conexão — `ForeignKeyViolationError`,
tratado como falha terminal mesmo quando o envio já tinha sido aceito pela plataforma.
**Corrigido** nessa mesma rodada (item 18). Essa correção também revelou que o item 3
(perda no retry) passava a ser **necessário**, não mais opcional: com o vazamento de
sonda corrigido, o caminho de adiamento se abriu, e é exatamente esse caminho que o bug
da routing key destrói.

**Esta terceira rodada** corrigiu o item 3 (routing key do retry — item 14) e dois
defeitos que não estavam no código de produção, mas no **desenho da suíte de testes**:

5. **Critério de aceite do C-3 mal especificado** — a hipótese central ("o pico não
   cresce com o número de workers") é falsificável só por crescimento, mas o critério
   usava uma banda bilateral (`max/min ≤ 1.25×`) que reprovava também quedas do pico —
   compatíveis com a própria hipótese. Corrigido para um limite unilateral contra a
   linha de base.
6. **O contrafactual do Cenário A (A-4) não testava o que deveria testar com 1 worker**
   — investigado a fundo, com três hipóteses sucessivas eliminadas por medição (ver
   § 1.6). Nesta rodada, também foi descoberto e corrigido um **bug real no próprio
   código de teste** (`tests/integration/test_messaging.py`): dois helpers de polling
   reusavam o mesmo canal AMQP que declarou a fila originalmente, e o `aio_pika`
   devolve, nesse caso, a contagem em cache do momento da declaração — nunca a
   contagem real do servidor. Confirmado comparando com `rabbitmqctl` e com a API de
   management do RabbitMQ, que sempre mostraram o número correto no mesmo instante em
   que os helpers, pelo canal antigo, insistiam em zero. **Não é o mesmo bug do item
   14** (que é sobre a routing key do dead-letter) — é um defeito independente na
   forma como o teste consulta o broker, e explica por que
   `test_degrau_fora_da_faixa_e_limitado` parecia "flaky por contenção da VM" na rodada
   anterior: não era a VM, era o canal reusado.

**Quarta rodada** (esta): o A-4 da rodada anterior tinha ficado sem solução — três
hipóteses eliminadas por medição concluíram que o teto de vazão agregada desta VM
(~6-8 req/s) ficava **abaixo** do limite atribuído ao YouTube (20 req/s), então o
contrafactual (desligar o rate limiter) nunca gerava demanda suficiente para superar
o limite da plataforma. Isso não era só "uma limitação do ambiente a registrar" — era
um **confundidor da hipótese central**: o platô do Cenário C também nunca chegava
perto de 16 req/s, e não havia medição que separasse "o platô é o mecanismo" de "o
platô é o teto da VM". Esta rodada recalibrou **somente o YouTube**
(`allowed_rps` 16→3, `burst_capacity` 3→1, `estimated_limit_rps` 20→5 — todos abaixo
do teto medido do ambiente) nos quatro lugares onde o número precisa ficar
sincronizado: `.env`/`.env.example`, `src/apt/domain/platforms.py`, o seed de
`platform_thresholds` em `db/migrations/001_init.sql`, e o limite que o próprio
simulador aplica (que lê `estimated_limit_rps` do mesmo perfil de domínio). O
Instagram não mudou — ver TRADE-OFFS.md item 19 para a análise completa.

7. **Segundo confundidor, descoberto ao medir o resultado da recalibração, não
   previsto:** a primeira execução pós-recalibração do `load_test.py` mostrou pico de
   apenas ~2 req/s em **ambos** os cenários (com e sem proteção) — abaixo até do
   próprio `allowed_rps` de 3. A causa não era a calibração (confirmada correta via
   `GET /platforms` e `/admin/stats` antes de investigar mais). Era a estratégia de
   jitter `humanized`, padrão de `create_campaign()`: ela multiplica a demanda pelo
   perfil de atividade da **hora do dia** (`jitter.py::HOURLY_ACTIVITY_PROFILE`), e a
   execução aconteceu às ~05h UTC, dentro do intervalo 0h-5h em que esse
   multiplicador cai para 0.12-0.25. Um teste de carga controlado não pode ter seu
   resultado dependente do relógio de parede — corrigido trocando para
   `jitter_strategy="uniform"` nos dois scripts (`load_test.py`, `scale_test.py`),
   que passa a ignorar a hora do dia e manter a demanda proporcional apenas a
   `target_rate_per_min`. Ver § 1.6 para o resultado antes e depois desta correção.
8. **O critério unilateral do C-3 (rodada anterior) contradizia uma invariante do
   próprio projeto** — `burst_capacity + allowed_rps ≤ estimated_limit_rps`
   (`test_domain.py`) autoriza `1 + 3 = 4` req/s para o YouTube recalibrado, mas a
   tolerância relativa de `1.15×` sobre uma linha de base de 3 req/s (`3.45`) proíbe
   esse mesmo `4`. Corrigido substituindo o critério relativo por medição direta
   contra o teto algébrico do bucket (`peak_rps ≤ allowed_rps + burst_capacity`, em
   toda configuração) — a segunda correção de critério do projeto, pelo mesmo motivo
   da primeira: um proxy que funciona numa escala pode quebrar silenciosamente
   noutra. Ver § 1.8 e TRADE-OFFS.md item 20.

---

## 1.1 Qualidade estática

Ambiente: Python 3.12.13, Ubuntu 22.04.5 LTS (kernel 6.8.0-124-generic), **4 vCPU**,
7.7 GiB RAM — VM **compartilhada** com outras cargas de trabalho e com processos de
desktop (VS Code, GNOME Shell) sempre ativos. Isso importa: o teto de vazão agregada
medido no Cenário A (§ 1.6) é da ordem de 6-10 req/s mesmo escalando workers — bem
abaixo do que uma máquina dedicada com mais núcleos produziria.

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

## 1.2 Testes unitários

```
$ pytest tests/unit -q
........................................................................
...............................................
119 passed in 1.9s
```

| Arquivo | Testes | O que cobre |
|---|---|---|
| `test_retry.py` | 28 | Backoff com full jitter, escolha de degrau, status retentáveis |
| `test_domain.py` | 20 | Contrato da mensagem, `Outcome`, invariantes dos perfis |
| `test_jitter.py` | 18 | Distribuição temporal, perfil diário, convergência de taxa fracionária |
| `test_token_bucket.py` | 17 | Casos de borda do rate limiter |
| `test_breaker_state.py` | 13 | Transições do circuito, respostas atrasadas, liberação de sonda |
| `test_platform_sim.py` | 12 | Janela deslizante, `peak_rps`, expiração de falha |
| `test_bulkhead.py` | 11 | Isolamento entre plataformas, não vazamento de slot |
| **Total** | **119** | **~2 s, sem nenhuma infraestrutura** |

## 1.3 Cobertura

Inalterada nos números totais pelas correções desta rodada — `topology.py` e
`publisher.py` são fachadas de I/O, cobertas pelos testes de integração, não pelos
unitários.

| Módulo | Statements | Cobertura |
|---|---|---|
| `resilience/token_bucket.py` | 31 | **100%** |
| `resilience/retry.py` | 25 | **100%** |
| `platform_sim/throttle.py` | 55 | **100%** |
| `resilience/breaker_state.py` | 58 | **98%** |
| `scheduling/jitter.py` | 60 | **98%** |
| `domain/models.py` | 99 | **98%** |
| `domain/platforms.py` | 22 | **95%** |
| `observability/metrics.py` | 23 | **91%** |
| `resilience/bulkhead.py` | 58 | **86%** |
| `config.py` | 68 | **81%** |

## 1.4 Estrutura do repositório

| Métrica | Valor |
|---|---|
| Arquivos Python de produção | 44 |
| Arquivos Python de teste | 20 |
| Testes unitários | 119 |
| Testes de integração | 54 (em 4 arquivos) |
| Cenários de carga/resiliência/escala | 3 |
| Scripts Lua | 2 |
| ADRs | 12 (+ índice) |
| Serviços no Docker Compose | 7 |
| Rotas na API | 21 |
| Métricas Prometheus | 11 |

## 1.5 Testes de integração

```
$ pytest tests/integration -v -m integration
...
1 failed, 44 passed, 9 skipped in ~10s
```

Passou de **42 passed, 3 failed** (rodada anterior) para **44 passed, 1 failed** — as
duas falhas resolvidas eram, respectivamente, o bug real do item 14 (agora corrigido) e
o bug no helper de teste descrito na § 1.0 (agora também corrigido). Reproduzido de
forma estável em execuções repetidas, com estado limpo entre elas.

| Teste | Causa raiz | Status |
|---|---|---|
| `test_pausa_e_retoma` | Gap fixture × produção: a fixture `client` cria a app sem lifespan (de propósito, para não iniciar o dispatcher) e o singleton `Publisher` nunca é conectado; `pause_campaign`/`resume_campaign` precisam dele. Código correto, fixture incompleta para este caso específico. | Falha conhecida, não corrigida (fora do escopo desta rodada) |
| `test_retry_volta_para_a_fila_original` | Pegava o bug do item 14 (routing key incorreta no dead-letter). **Agora passa** — a mensagem retorna a `apt.tasks.youtube` com o `attempt` intacto. | **Corrigido na rodada anterior** |
| `test_degrau_fora_da_faixa_e_limitado` | Não era "contenção da VM" como avaliado antes — era o bug do helper de polling (§ 1.0) reusando o canal de declaração. **Agora passa.** | **Corrigido na rodada anterior** |

### Um vazamento de estado descoberto ao tentar reproduzir o resultado acima

Rodando a suíte completa nesta rodada, `test_retry_volta_para_a_fila_original` voltou
a falhar de forma **intermitente**, mas por um motivo diferente do item 14: a
mensagem retornava a `apt.tasks.youtube`, só que com um `task_id` de **outra**
campanha, gerado por outro teste. Rastreado até `TestPausarRetomar::test_pausa_e_retoma`
e `TestConsulta::test_lista_filtrando_por_status`, que criam campanhas com
`activate=True` e nunca as pausam. A fixture `client` deste módulo roda a app **sem**
lifespan de propósito (para o dispatcher em processo não interferir na contagem — ver
o comentário no topo do arquivo), mas isso não isola o teste do dispatcher do
container `api` do `docker compose`, que já está no ar e aponta para o **mesmo**
Postgres: uma campanha ativada por um teste é despachada de verdade por aquele
processo, publicando mensagens reais nas filas compartilhadas até alguém pausá-la —
inclusive depois que o teste que a criou já terminou. Corrigido com uma fixture
`autouse` em `test_api_campaigns.py` que pausa qualquer campanha `active` ao final de
cada teste do módulo (e chama `dispose_engine()` depois, porque a limpeza cria um
engine novo preso ao event loop do teste — sem isso, o teste seguinte herdaria um
engine morto e o módulo inteiro passaria a "skippar" por "Postgres indisponível").
Reproduzido de forma estável em duas execuções completas após a correção: **44
passed, 1 failed (conhecida), 9 skipped**, sem intermitência.

### O bug do helper de teste, em detalhe

`_wait_for_message_count` e `_poll_queue` (`tests/integration/test_messaging.py`)
recebiam um objeto `Queue` já vinculado ao canal que fez a declaração original (não
passiva) daquela fila, e consultavam a contagem chamando
`queue.channel.declare_queue(name, passive=True)` repetidamente no mesmo canal.
Isolado fora do pytest: publicar uma mensagem e consultar a contagem por esse canal
retorna **zero indefinidamente**, mesmo com a mensagem genuinamente presente e
confirmada — `rabbitmqctl list_queues` e a API de management do RabbitMQ, consultados
no mesmo instante, sempre mostraram a contagem certa. Abrir um canal **novo** (mesma
conexão ou uma nova) resolve — a contagem aparece na primeira tentativa. Corrigido
abrindo uma conexão nova dedicada à consulta em cada um dos dois helpers.

## 1.6 Cenário A — Carga

Comando: `python -m tests.load.load_test`. O script escala para **5 workers** antes de
rodar os dois cenários. Nesta rodada, dois parâmetros do script mudaram além dos
números do YouTube: `jitter_strategy="uniform"` (ver § 1.0, item 7) e volume reduzido
de 400 para 150 envios por cenário (o mesmo volume a 3 req/s, em vez de 16, levaria
~5.3× mais tempo para escoar).

### Primeira execução pós-recalibração: confundidor novo, não o esperado

Com os quatro pontos de calibração confirmados corretos via `GET /platforms` e
`GET localhost:9001/admin/stats` (`youtube: allowed=3.0, burst=1, limite=5.0` — exatamente
o valor configurado), a primeira execução mostrou pico de **2-3 req/s em ambos os
cenários**, inclusive no desprotegido — abaixo até do próprio `allowed_rps`. Isso
significaria que a calibração "não pegou" em algum lugar, exceto que os quatro lugares
já estavam confirmados certos. A causa real, descrita em detalhe na § 1.0 item 7: a
estratégia de jitter padrão (`humanized`) modula a demanda pela hora do dia, e a
execução aconteceu dentro da janela de baixa atividade (0h-5h UTC, multiplicador
0.12-0.25) — um confundidor **diferente** do teto de ambiente, específico do horário
em que o script roda, não da VM em si. Corrigido trocando para
`jitter_strategy="uniform"` nos dois scripts de carga. Os números abaixo já refletem
essa correção.

### Teste de carga -- youtube

Demanda solicitada: **600/min (10 req/s)** | nosso limite: **3.0 req/s** | limite da plataforma: **5 req/s** | **5 workers**

| Metrica | Protecoes ligadas | Rate limiter desligado |
|---|---|---|
| Envios aceitos (2xx) | 150 | 235 |
| **429 recebidos da plataforma** | **0** | **133** |
| Adiados por nos (rate_limited_local) | 3332 | 3332 (cumulativo — item 17; ver nota) |
| Pico observado pela plataforma (req/s) | 4 | 5 |
| Limite da plataforma (req/s) | 5 | 5 |
| 429 devolvidos pela plataforma | 0 | 133 |

**Latencia (cenario protegido)**

| Percentil | Latencia (ms) |
|---|---|
| p50 | 28.0 |
| p95 | 47.1 |
| p99 | 54.0 |
| max | 57.0 |
| amostras | 150 |

| Critério de aceite | Esperado | Medido | Resultado |
|---|---|---|---|
| A-1 · com proteção, 429 recebidos | `= 0` | **0** | OK |
| A-2 · pico observado ≤ limite da plataforma | `≤ 5 req/s` | 4 req/s | OK |
| A-3 · excedente foi adiado, não descartado | `rate_limited_local > 0` | **3332** | OK |
| A-4 · contrafactual: sem proteção os 429 aparecem | `throttled(sem) > throttled(com)` | 133 > 0 | **OK — recalibração resolveu o A-4** |

**Nota sobre `rate_limited_local`:** o mesmo número (3332) aparece nos dois cenários
porque `/admin/outcomes` soma toda a tabela `executions` sem filtro de campanha
(TRADE-OFFS.md item 17, já documentado antes desta rodada) — o segundo cenário herda
o total acumulado do primeiro. O `sent` do cenário desprotegido (235) tem o mesmo
problema (150 do primeiro + 85 do segundo). Os números que **não** sofrem esse efeito
— `peak_rps`, `total_throttled`, `total_accepted` do simulador — são os que sustentam
os critérios de aceite acima, e esses são isolados corretamente por `POST
/admin/reset` entre cenários.

### A-4 passa: o contrafactual volta a produzir 429 de verdade

Com o rate limiter desligado, a mesma demanda (600/min, 5 workers) que ficava em 3-4
req/s com a proteção ligada subiu para um pico de **5 req/s — exatamente o limite do
simulador** — e a plataforma devolveu **133 respostas 429**. Religar a proteção fez
os 429 desaparecerem de novo. É o resultado que a recalibração (TRADE-OFFS.md item
19) existia para produzir: o teto de ambiente (~6-8 req/s) agora **excede** o limite
do simulador (5 req/s), então desligar o rate limiter finalmente expõe demanda
suficiente para ultrapassá-lo.

### Efeito colateral descoberto: o circuit breaker entra em jogo no cenário desprotegido

O cenário "rate limiter desligado" demorou mais que o timeout de 240s do script
(`wait_for_campaign`) para drenar completamente, e terminou com `deferred: 59` e
`dead: 6` ainda pendentes no momento em que o script desistiu de esperar e seguiu para
o relatório. Causa: o circuit breaker (que segue **ligado** neste cenário — só a flag
`rate_limiter_enabled` foi desativada) trata `429` como falha, e a sequência de 133
respostas 429 foi suficiente para abrir o circuito do YouTube (`circuit_open: 1132`
nos outcomes registrados) — que por sua vez passou a **adiar** novos envios em vez de
tentá-los, prolongando a campanha. Não é um bug: é o segundo mecanismo de proteção
(circuit breaker) agindo como rede de segurança quando o primeiro (rate limiter) está
desligado — **defesa em profundidade**, funcionando como projetado, mesmo que não
fosse o efeito que este cenário específico pretendia isolar. Os 6 envios que
esgotaram `max_attempts=4` geraram as primeiras entradas **genuínas e não-forçadas**
de `failures` do projeto (`last_outcome: throttled`, `last_error: "rate limit da
plataforma (429)"`) — ver § 1.7 para o exercício deliberado do mesmo caminho com o
Instagram.

### Duração

`load_test.py` completo: **5m30s** (a maior parte no cenário desprotegido, por causa
do efeito acima). Sem esse efeito colateral, a duração esperada seria próxima de 1-2
minutos por cenário. Dentro do orçamento aceitável para a demo, mas no limite —
registrado aqui para quem for medir de novo.

## 1.7 Cenário B — Resiliência

**Hipóteses:** H1 — o circuito do Instagram abre, sonda e fecha sozinho. H2 — o
YouTube continua enviando durante a falha do Instagram.

Comando: `python -m tests.load.resilience_test`. Falha injetada: `error_500` no
**instagram** em t≈8s, com TTL de 25s (auto-expira).

### Teste de resiliencia

**Linha do tempo**

| t (s) | Circuito Instagram | Circuito YouTube | Aceitas YT | Aceitas IG |
|---|---|---|---|---|
| 0.0 | closed | closed | 0 | 0 |
| 6.1 | closed | closed | 11 | 8 |
| 12.1 | **open** | closed | 18 | 11 |
| 18.2 | open | closed | 28 | 11 |
| 24.2 | open | closed | 36 | 11 |
| 30.3 | open | closed | 45 | 11 |
| 36.3 | open | closed | 53 | 11 |
| 42.4 | half_open | closed | 61 | 13 |
| 48.4 | **closed** | closed | 69 | 34 |
| 54.5 | closed | closed | 80 | 53 |

**Transições do circuit breaker**

| Plataforma | De | Para | Motivo |
|---|---|---|---|
| instagram | closed | open | error |
| instagram | open | half_open | allow |
| instagram | half_open | open | error |
| instagram | open | half_open | allow |
| instagram | half_open | closed | sucesso |

Mesmo ciclo completo observado na rodada anterior, reproduzido de forma estável: abriu,
sondou, uma sonda encontrou a falha ainda ativa e reabriu, sondou de novo, e na segunda
tentativa a falha já tinha expirado — fechou.

**Isolamento (bulkhead):** o YouTube aceitou **34** envios durante a janela em que o
Instagram estava fora do ar, sem qualquer efeito visível da falha.

| Critério de aceite | Esperado | Medido | Resultado |
|---|---|---|---|
| B-1 · o circuito do Instagram abriu | `open` presente | presente (t≈12s) | OK |
| B-2 · o circuito sondou | `half_open` em `breaker_events` | presente, duas vezes | OK |
| B-3 · o circuito fechou após a falha expirar | estado final `closed` | **`closed`** | OK |
| B-4 · o circuito do YouTube nunca abriu | `open` ausente | ausente | OK |
| B-5 · o YouTube seguiu enviando durante a falha | envios `> 0` | 34 | OK |

**Todos os cinco critérios passam.**

### DLQ e `failures`: vazias pelo motivo certo agora

```sql
SELECT count(*) FROM failures;  -- 0
```
```
$ rabbitmqctl list_queues name messages | grep dlq
apt.dlq   0
```

Na rodada anterior, a DLQ e `failures` vazias **apesar de** falhas reais confirmavam a
perda silenciosa do item 14. Nesta rodada, com o bug corrigido, a mesma consulta na
tabela `send_tasks` conta uma história diferente:

```sql
SELECT status, COUNT(*) FROM send_tasks
WHERE campaign_id IN (SELECT id FROM campaigns WHERE name LIKE 'Resiliencia%')
GROUP BY status;
-- sent: 254, pending: 3 (orçamento ainda não consumido, campanha continua ativa)
```

Zero `deferred`, zero `failed`. As filas e a tabela ficam vazias porque **as tarefas
adiadas durante a falha do Instagram voltaram e tiveram sucesso**, não porque
desapareceram — a mesma distinção que a § 1.6 documenta para o Cenário A.

### O caminho da DLQ, exercitado deliberadamente pela primeira vez

Este cenário (a falha expira sozinha em 25s, antes de qualquer tarefa esgotar
`max_attempts=4`) nunca tinha exercitado `failures`/`apt.dlq` de fato — eram sempre
vazias, e até esta rodada não havia evidência de runtime de que o caminho funcionava,
só de que ele não tinha sido acionado. Exercitado manualmente nesta rodada com o
circuit breaker **desligado** (essencial: com ele ligado, o circuito abre e passa a
**adiar** — adiamento não consome `attempt`, são contadores distintos de propósito) e
uma falha `error_500` no Instagram sem TTL (persistente até removida manualmente):

```bash
curl -X PATCH localhost:8000/flags/circuit_breaker_enabled -d '{"value":false}'
curl -X POST localhost:9001/admin/fault -d '{"platform":"instagram","mode":"error_500"}'
# campanha de 12 envios no Instagram
```

Resultado: **as 12 tarefas foram para `dead`** (`send_tasks.attempts = 4` em todas),
com 12 linhas correspondentes em `failures` —

```
last_outcome: error
last_error:   HTTP 500: {"error":"erro interno da plataforma (falha injetada)"}
total_attempts: 4
```

**O caminho funciona.** `attempt` final = 4 (`= max_attempts`) em todos os casos;
`last_error` reflete a causa real (o 500 injetado), não um erro genérico. Registrado
aqui como o primeiro exercício real deste caminho — recomenda-se incorporá-lo como
cenário formal em PLANO-DE-TESTES.md numa rodada futura, com hipótese e critério
próprios, em vez de permanecer apenas como um exercício manual.

**Efeito colateral, não relacionado ao Instagram:** o cenário desprotegido do
Cenário A (§ 1.6) também gerou `failures` genuínas para o **YouTube** (6 entradas,
`last_outcome: throttled`), pela primeira vez de forma não-forçada — confirmando o
mesmo caminho por um gatilho diferente (429 da plataforma, não erro 500 injetado).

## 1.8 Cenário C — Escala

> **Este é o teste mais importante do projeto** — a prova de que o rate limiter é
> distribuído. Nesta rodada, **todos os critérios passam**, C-3 incluído, depois da
> segunda correção de critério do projeto — ver a análise abaixo.

Comando: `python -m tests.load.scale_test`. Volume reduzido de 500 para 150 envios por
configuração e `jitter_strategy="uniform"`, pelos mesmos motivos do Cenário A (§ 1.6).

### Teste de escala -- a prova do rate limiter distribuido

Demanda solicitada: **600/min (10 req/s)** | nosso limite: **3.0 req/s** | limite da plataforma: **5 req/s**

| Workers | Aceitos (real, por campanha) | **429** | Adiados (real, por campanha) | **Pico observado pela plataforma** | Duracao (s) |
|---|---|---|---|---|---|
| 1 | 150 | **0** | 2664 | **3/s** | 64.5 |
| 3 | 150 | **0** | 3582 | **3/s** | 76.6 |
| 5 | 150 | **0** | 3486 | **4/s** | 66.7 |

**Aceitos e adiados acima já estão isolados por `campaign_id` diretamente no
Postgres** — o relatório impresso pelo script mostra os totais cumulativos (150, 300,
450 aceitos; 2664, 6246, 9732 adiados), porque `/admin/outcomes` soma toda a tabela
sem filtro de campanha (item 17, o mesmo efeito documentado na § 1.6). Os dois
conjuntos concordam depois de descontar a acumulação: 6246−2664=3582,
9732−6246=3486.

**O pico observado coincide exatamente com o teto algébrico do token bucket
compartilhado (`allowed_rps + burst_capacity = 3 + 1 = 4`) e não o ultrapassa em
nenhuma configuração — 1, 3 ou 5 workers.**

| Critério de aceite | Esperado | Medido | Resultado |
|---|---|---|---|
| C-1 · pico ≤ limite em todas as configurações | `≤ 5 req/s` | 3/3/4 req/s | **OK** |
| C-2 · zero 429 em todas | `= 0` | 0/0/0 | **OK** |
| C-3 · o pico não excede o teto algébrico do bucket, em nenhuma configuração | `≤ 4 req/s` | 3/3/4 req/s | **OK** |
| C-4 · ≥ 2 réplicas com trabalho, razão ≤ 4.0 | — | ver análise (números do script são cumulativos) | **OK, isolando por campanha** |

### C-3 — segunda correção de critério do projeto, e por quê

**A primeira correção** (rodada anterior) trocou uma banda bilateral por uma
unilateral: a hipótese central *"o limite é global, então escalar workers não aumenta
o pico"* só é falsificável por crescimento, então a comparação passou a ser contra a
linha de base (1 worker), só na direção de crescimento, com tolerância de 15%.

**Esta rodada corrigiu o critério unilateral em si**, porque ele contradizia uma
invariante que o próprio projeto declara. `test_domain.py::
test_burst_mais_refill_nao_passa_do_limite_estimado` afirma
`burst_capacity + allowed_rps ≤ estimated_limit_rps` — o projeto autoriza
explicitamente `1 + 3 = 4` req/s numa única janela do simulador para o YouTube
recalibrado (TRADE-OFFS.md item 19). Um teto relativo de `1.15×` sobre a linha de base
de 3 req/s é `3.45` — **proíbe o próprio `4` que a especificação do burst permite**. O
pico de 4 req/s medido com 5 workers é exatamente esse caso: real, esperado, e ainda
assim reprovado pelo critério relativo (`1.33×` contra `1.15×` de tolerância) na
primeira medição desta rodada. A causa raiz não é o mecanismo — é que o menor
incremento inteiro possível (1 req/s) já é 33% de um baseline de 3 req/s, mas era só
6% do baseline de 16 req/s para o qual a tolerância de 15% tinha sido calibrada. **O
critério estava errado, não a medição** — a mesma lição do item 6 (jitter): um proxy
que funciona numa escala pode quebrar silenciosamente em outra.

**O critério atual mede a hipótese diretamente**, contra os dois tetos que ela
realmente afirma: o pico nunca excede o teto algébrico do bucket compartilhado (que
**não** cresce com o número de workers, porque o estado é compartilhado) e nunca
excede o limite da plataforma (C-1). Um rate limiter em memória de processo violaria o
primeiro teto exatamente ao escalar — 5 processos, 5 baldes, `5 × 4 = 20` req/s
possíveis, contra o `4` medido. É essa violação — não uma razão percentual — que o
critério agora detecta. A razão relativa contra a linha de base continua disponível
como informação no script (fora do veredito), aplicável apenas quando a linha de base
for `≥ 20 req/s`. TRADE-OFFS.md item 20 registra o histórico completo desta correção,
incluindo por que a tolerância de 15% não foi simplesmente alargada.

### C-4 — a razão real (isolando por campanha) fica bem dentro da tolerância

O critério do script usa `/admin/workers`, que soma toda a tabela sem filtro de
campanha (item 17) e por isso mistura réplicas de configurações diferentes —
exatamente o efeito que produziu o `FALHOU` (razão 6.72×) no relatório impresso.
Isolando por `campaign_id` diretamente no Postgres:

| Workers | Distribuição real por réplica (envios `sent`) | Razão maior/menor |
|---|---|---|
| 3 | 54 / 52 / 44 | **1.23×** |
| 5 | 37 / 34 / 34 / 25 / 20 | **1.85×** |

Ambos dentro da tolerância de 4.0×. A razão do cenário de 5 workers (1.85×) é maior
que a da rodada anterior com o volume antigo (1.02×) — esperado, dado que o volume por
campanha caiu de 500 para 150: com menos envios totais por réplica, a mesma variação
absoluta de tarefas produz uma razão relativa maior. Ainda assim, folga confortável
sobre o limite de 4.0×.

### A conclusão do experimento: 100% de conclusão em toda configuração

```sql
SELECT c.name, COUNT(*) FILTER (WHERE t.status = 'sent') AS sent, COUNT(*) AS total
FROM send_tasks t JOIN campaigns c ON c.id = t.campaign_id
WHERE c.name LIKE 'Escala%' GROUP BY c.name ORDER BY c.name;
-- Escala - 1 workers: sent=150, total=150
-- Escala - 3 workers: sent=150, total=150
-- Escala - 5 workers: sent=150, total=150
```

**Todos os 450 envios solicitados (150 por configuração) completaram, em 1, 3 e 5
workers**, apesar de até 3582 adiamentos numa única campanha (mais de 23 adiamentos
por tarefa em média, no cenário de 3 workers). Zero perdidos, consistente com a
correção do item 14 continuando válida sob a nova calibração.

## 1.9 Consultas PromQL para a demonstração

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

Três coisas que valem ser ditas na apresentação sobre esta suíte.

**Um número vermelho pode ter causa raiz no próprio teste, não no sistema.** As duas
falhas de integração desta rodada anterior eram, na verdade, uma no sistema (item 14,
corrigida) e uma no **helper de teste** (§ 1.0/§ 1.5, também corrigida) — inicialmente
mal-diagnosticada como "flakiness por contenção da VM". A lição: antes de aceitar uma
explicação ambiental para um teste vermelho, reproduza fora do framework de teste,
comparando com uma fonte de verdade independente (aqui, `rabbitmqctl` e a API de
management do RabbitMQ).

**Nem todo cenário sobrevive à correção de outro bug.** A-4 não passa não porque o
mecanismo esteja errado, mas porque esta VM específica não gera throughput agregado
suficiente para desafiar o limite da plataforma em nenhuma configuração testada — um
limite de **ambiente**, descoberto eliminando três hipóteses por medição direta (§ 1.6),
não assumido.

**A evidência mais forte vem do lado de fora.** O `peak_rps` é o pico que a
**plataforma** observou, medido por ela — não o que o sistema registra sobre si mesmo.
É também a métrica que o artefato de acumulação do item 17 não contamina (resetada a
cada cenário), o que a torna a base confiável para C-1 e C-3, os dois critérios que
sustentam a tese central do projeto.
