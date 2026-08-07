# Plano de testes

Documento que define **o que** é testado, **por que** aquele teste prova algo, e **qual é
o critério de aceite**. Os resultados medidos ficam em
[RESULTADOS-TESTES.md](RESULTADOS-TESTES.md).

## Princípio que organiza a suíte

A pirâmide de testes aqui não é convenção — ela decorre de uma decisão de projeto. A
lógica crítica do sistema (token bucket, máquina de estados do breaker, jitter, backoff)
foi escrita como **função pura**: sem I/O, recebendo o tempo como parâmetro.

Consequência: os casos de borda que mais importam podem ser testados **sem nenhuma
infraestrutura**, em milissegundos, no CI.

```
              ┌─────────────────────┐
              │  carga / resiliência │  3 cenários · stack completo
              │      / escala        │  produzem RELATÓRIO
              ├─────────────────────┤
              │     integração       │  54 testes · Postgres + Redis + RabbitMQ
              │                     │  verificam o que só a infra real revela
              ├─────────────────────┤
              │     unitários        │  117 testes · SEM infraestrutura
              │                     │  casos de borda exaustivos
              └─────────────────────┘
```

---

## Nível 1 — Testes unitários (117 testes, sem infraestrutura)

```bash
pytest tests/unit -v
```

### `test_token_bucket.py` — 17 testes

O arquivo mais importante da suíte. O token bucket é o mecanismo central da POC, e como a
versão que roda em produção é um script Lua, a garantia de correção do **algoritmo** vem
daqui.

| Teste | O que prova | Por que importa |
|---|---|---|
| `test_bucket_cheio_nao_passa_da_capacidade` | Fichas nunca excedem `capacity` | Sem o limite, um bucket parado 1h acumularia 57.600 fichas e liberaria uma rajada gigantesca |
| `test_relogio_para_tras_nao_remove_fichas` | `elapsed` negativo não debita | Correção de NTP entre containers produziria limiter intermitentemente mais restritivo — **impossível de diagnosticar** |
| `test_negativa_nao_consome_credito` | Negar mantém o saldo | Se consumisse, um cliente insistente atrasaria os outros indefinidamente |
| `test_retry_after_permite_a_proxima_tentativa` | Esperar `retry_after_ms` é suficiente | Se o prazo fosse curto por 1ms, o cliente voltaria cedo e seria negado de novo — ciclo de tentativas inúteis |
| `test_pedido_maior_que_capacidade_e_negado_sem_prazo` | `retry_after=0` | Devolver prazo seria mentir: por mais que se espere, nunca haverá 20 fichas num bucket de 16 |
| `test_rajada_esgota_bucket_e_depois_converge` | 20 instantâneas contra capacidade 16 → exatamente 16 passam | A propriedade central do algoritmo |
| `test_vazao_sustentada_respeita_o_limite` | 1000 tentativas em 10s (100 req/s de demanda contra 16 req/s) → total ≤ `capacity + rps×10` | A aritmética que sustenta o teste de escala |

### `test_breaker_state.py` — 11 testes

| Teste | O que prova |
|---|---|
| `test_sucesso_zera_o_contador_de_falhas` | O gatilho é "N falhas **consecutivas**", não somadas |
| `test_falha_atrasada_nao_reinicia_o_cooldown` | Resposta tardia não mantém o circuito aberto para sempre |
| `test_sucesso_atrasado_nao_fecha_o_circuito` | Informação mais antiga que a decisão de abrir é ignorada |
| `test_limita_as_sondas_simultaneas` | `half_open` não vira rajada sobre serviço recém-recuperado |
| `test_closed_open_half_open_closed` | O ciclo completo de recuperação |

### `test_bulkhead.py` — 11 testes

| Teste | O que prova |
|---|---|
| **`test_plataforma_esgotada_nao_afeta_a_outra`** | A propriedade central do padrão |
| **`test_timeout_nao_vaza_slot`** | Após 5 timeouts, a capacidade original permanece. Guarda o modo de falha mais perigoso: vazar 1 slot por timeout estreitaria o compartimento até a plataforma parar de ser atendida |
| `test_carga_concorrente_respeita_a_capacidade` | 20 corrotinas, no máximo 3 dentro |

### `test_jitter.py` — 18 testes

Testar código aleatório exige verificar **propriedades**, não valores. Todos usam `Random`
de semente fixa injetado — um teste que falha 1 em 50 execuções é pior que nenhum teste,
porque treina a equipe a ignorar vermelho.

| Teste | O que prova |
|---|---|
| `test_media_dos_intervalos_proxima_do_alvo` | A distribuição exponencial tem a média certa (tolerância 15%) |
| `test_offsets_sao_monotonicos` | O dispatcher publica na ordem que recebe |
| `test_taxa_fracionaria_converge_na_media` | 0.5 tarefa/tick × 2000 ticks ≈ 1000 envios |
| `test_sem_jitter_produz_rajada` | O modo contrafactual da demonstração funciona |
| `test_humanized_reduz_o_volume_de_madrugada` | O perfil diário modula na direção esperada |

### `test_retry.py` — 28 testes

| Teste | O que prova |
|---|---|
| **`test_full_jitter_produz_dispersao_alta`** | Desvio-padrão > 20% da média. Num backoff **sem** jitter o desvio seria **zero** — todos voltam no mesmo instante |
| `test_arredonda_para_cima` | Esperar mais é inofensivo; esperar menos volta antes da recuperação |
| `test_nao_retentaveis` | 400/401/403/404 vão direto para a DLQ |

### `test_domain.py` — 20 testes

| Teste | O que prova |
|---|---|
| **`test_defers_nao_incrementa_attempt`** | 50 adiamentos e `attempt` continua 0. Guarda a correção de um bug conceitual real |
| **`test_autolimitacao_nao_conta_como_rejeicao`** | Adiamentos não alimentam o breaker — se alimentassem, o rate limiter abriria o circuito ao fazer o seu trabalho |
| `test_allowed_rps_fica_abaixo_do_limite_estimado` | A margem de segurança é invariante, não preferência |
| `test_burst_nao_passa_do_limite_estimado` | Uma rajada sozinha não pode estourar o limite da plataforma |
| `test_campos_ausentes_usam_default` | Mensagens de versão anterior na fila não vão para a DLQ durante um deploy |

### `test_platform_sim.py` — 12 testes

O simulador é o **instrumento de medição** da POC. Se ele estiver errado, todos os
resultados de carga estão errados.

| Teste | O que prova |
|---|---|
| `test_janela_desliza_com_o_tempo` | A expiração da janela (relógio controlado por monkeypatch) |
| `test_peak_rps_registra_o_maximo_observado` | O pico histórico **não diminui** quando a janela esvazia |
| `test_falha_com_ttl_expira` | A auto-expiração de que o teste de resiliência depende |

---

## Nível 2 — Testes de integração

```bash
docker compose up -d
pytest tests/integration -v -m integration
```

Sem infraestrutura, fazem **skip** em vez de falhar. A distinção importa: falha significa
"o código está errado", skip significa "não foi possível verificar aqui".

### `test_rate_limiter_redis.py`

| Teste | Hipótese | Critério de aceite |
|---|---|---|
| **`TestConcorrencia::test_concorrencia_nao_estoura_o_limite`** | O script Lua é atômico | 50 corrotinas simultâneas, timestamp fixo, capacidade 10 → **exatamente 10** permitidas. Valor maior = perda de atomicidade |
| **`TestParidade::test_paridade_com_a_implementacao_de_referencia`** | Lua e Python concordam | 30 passos de 100ms: `allowed` idêntico, `tokens` ±0.01, `retry_after` ±1ms |
| `test_eixo_do_conteudo_limita_url_unica` | O eixo por conteúdo é o gargalo numa URL só | `limited_by == "content"` e aceitas ≤ 8 |
| `test_urls_distintas_nao_compartilham_bucket` | Distribuir aumenta a vazão aproveitável | 20 URLs → ≥ 15 aceitas |
| `test_plataformas_tem_buckets_independentes` | Esgotar YouTube não afeta Instagram | Instagram permitido |
| `test_ttl_e_aplicado` | A faxina automática de chaves funciona | `0 < TTL ≤ 120` |

### `test_circuit_breaker_redis.py`

| Teste | Hipótese | Critério de aceite |
|---|---|---|
| **`test_falhas_contadas_coletivamente`** | O estado é compartilhado entre processos | 5 instâncias distintas, **1 falha cada** → circuito abre. Uma 6ª que nunca viu falha já encontra `open` |
| `test_circuitos_por_plataforma_sao_independentes` | Junção com o bulkhead | Instagram `open`, YouTube `closed` |
| `test_concorrencia_nao_perde_contagem` | Transições atômicas | N falhas simultâneas, todas contadas |

### `test_messaging.py`

| Teste | Hipótese | Critério de aceite |
|---|---|---|
| **`test_retry_volta_para_a_fila_original`** | O backoff acontece no broker, preservando a routing key | Mensagem aparece em `apt.retry.1`, e após o TTL reaparece em `apt.tasks.youtube` com `attempt` intacto |
| **`test_todas_as_filas_recebem_o_evento`** | Fanout entrega a todos | 2 filas ligadas recebem a mesma mensagem. Com topic, apenas 1 receberia |
| `test_mensagem_e_persistente` | Sobrevive a restart do broker | `delivery_mode == PERSISTENT` |
| `test_declaracao_e_idempotente` | API e worker declaram sem coordenação | Duas chamadas, mesmo resultado |

### `test_api_campaigns.py`

Roda contra o Postgres **real** — é o que exercita constraints, ENUMs e `ON CONFLICT`.

| Teste | Critério de aceite |
|---|---|
| `test_activate_true_ja_nasce_ativa` | Criação + pool + ativação na mesma transação |
| `test_recusa_pool_vazio` | 422 (campanha sem URL não tem o que enviar) |
| `test_recusa_urls_duplicadas` | 422 (evita sobrescrita silenciosa de peso) |
| `test_ready_reporta_cada_check` | O corpo detalha **qual** verificação falhou |

---

## Nível 3 — Testes de carga, resiliência e escala

Não usam pytest: são scripts que produzem **relatório**. Ainda assim avaliam critérios de
aceite explícitos e retornam exit code ≠ 0 quando algum falha.

### Cenário A — Carga (`python -m tests.load.load_test`)

**Hipótese.** Submetido a demanda muito acima do limite, o sistema (a) não recebe 429, (b)
mantém a vazão abaixo do `allowed_rps`, e (c) registra o excedente como **adiamento**, não
como falha.

**O item (c) é o mais importante conceitualmente.** Um sistema que simplesmente derrubasse
o excedente também teria zero 429 — e teria **perdido trabalho**. O que provamos é
diferente: o excedente foi adiado e continua na fila.

**Desenho — dois cenários, para haver comparação:**

| Cenário | Configuração |
|---|---|
| A1 | proteções **ligadas** |
| A2 | `rate_limiter_enabled = false` (via feature flag) |

**Sem o cenário A2, o resultado de A1 não significaria nada:** não daria para saber se os
429 não apareceram por causa do rate limiter ou porque a carga era baixa.

**Parâmetros:** YouTube · 400 envios · demanda 1800/min (30 req/s) · nosso limite 16 req/s ·
limite da plataforma 20 req/s · 8 URLs no pool.

**Critérios de aceite:**

| # | Critério | Como se mede |
|---|---|---|
| A-1 | Com proteção, **0** requisições 429 recebidas | `outcomes["throttled"] == 0` |
| A-2 | Pico observado pela plataforma ≤ limite dela | `sim.peak_rps ≤ 20` |
| A-3 | O excedente foi **adiado**, não descartado | `outcomes["rate_limited_local"] > 0` |
| A-4 | **Contrafactual:** sem proteção, os 429 aparecem | `throttled(A2) > throttled(A1)` |

Também coletado: p50/p95/p99 de latência dos envios aceitos.

### Cenário B — Resiliência (`python -m tests.load.resilience_test`)

**Duas hipóteses.**

**H1 — Circuit Breaker.** Quando o Instagram passa a devolver 500, o circuito daquela
plataforma **abre**; após a falha se resolver, ele **sonda** e **fecha** sozinho.

**H2 — Bulkhead.** Enquanto o Instagram está fora, o YouTube continua enviando na vazão
normal.

*H2 é a hipótese mais interessante,* porque um sistema sem bulkhead falharia de forma
**silenciosa**: nenhum erro apareceria, apenas a vazão do YouTube despencando por um motivo
que não tem nada a ver com o YouTube.

**Desenho:**

```
t=0     duas campanhas ativas (YouTube e Instagram), ambas saudáveis
t=8s    injeta error_500 no Instagram, com TTL de 25s
...     o circuito do Instagram abre; o YouTube segue
t=33s   a falha expira SOZINHA; o circuito sonda e fecha
t=60s   fim da observação
```

A falha tem TTL para que a recuperação aconteça **sem intervenção externa no meio da
medição** — o momento exato de uma chamada manual influenciaria o resultado.

**Critérios de aceite:**

| # | Critério | Como se mede |
|---|---|---|
| B-1 | O circuito do Instagram **abriu** | `"open"` na linha do tempo |
| B-2 | O circuito **sondou** (`half_open`) | `"half_open"` observado ou registrado em `breaker_events` |
| B-3 | O circuito **fechou** após a falha expirar | estado final `closed` |
| B-4 | O circuito do YouTube **nunca** abriu | `"open"` ausente nos estados do YouTube |
| B-5 | O YouTube seguiu enviando durante a falha | envios aceitos > 0 na janela de falha |

> **Nota sobre B-2.** `half_open` é transitório — dura apenas o tempo das sondas.
> Amostrando a cada 3s, é possível que ele exista e não seja capturado. As linhas de
> `breaker_events` são a evidência **definitiva**: elas registram todas as transições.

### Cenário C — Escala (`python -m tests.load.scale_test`)

> **O teste mais importante do projeto.**

**Hipótese.** Escalar de 1 → 3 → 5 workers aumenta a **capacidade de processamento**, mas
**não** a vazão enviada à plataforma.

**Por que isso prova algo.** Um rate limiter em memória de processo passaria em qualquer
teste com 1 worker. Com 5, cada um teria o seu balde de 16 req/s e o sistema enviaria
80 req/s — 4× o limite. O rate limiter existiria, estaria "funcionando" em cada processo, e
o sistema violaria o limite **exatamente ao escalar**.

**Parâmetros:** YouTube · 500 envios · demanda 2400/min (40 req/s) · 10 URLs · três
configurações de worker.

**Critérios de aceite:**

| # | Critério | Como se mede |
|---|---|---|
| C-1 | Em **todas** as configurações, pico ≤ limite da plataforma | `peak_rps ≤ 20` para 1, 3 e 5 workers |
| C-2 | Em **todas**, zero 429 | `throttled == 0` |
| C-3 | **O pico NÃO cresce com o número de workers** | variação entre menor e maior pico ≤ **1.25×** (limiter local daria ~5×) |
| C-4 | Load balancing: ≥ 2 réplicas recebem trabalho, razão maior/menor ≤ 4.0 | `GET /admin/workers` |

**C-3 é a hipótese central da POC.** A tolerância de 1.25× existe porque o jitter e o
desalinhamento entre a nossa janela e a do simulador produzem oscilação legítima.
Crescimento **linear** seria a assinatura inequívoca de um limiter por processo.

---

## Como executar tudo

```bash
# 1. Qualidade estática (sem infraestrutura)
ruff check src tests && ruff format --check src tests && mypy

# 2. Unitários (sem infraestrutura)
pytest tests/unit -v

# 3. Suba o stack
cp .env.example .env
docker compose up -d
docker compose ps          # todos healthy

# 4. Integração
pytest tests/integration -v -m integration

# 5. Carga, resiliência e escala (cada um imprime o relatório em markdown)
python -m tests.load.load_test
python -m tests.load.resilience_test
python -m tests.load.scale_test
```

## Política de reporte dos resultados

**Resultado ruim ou contra-intuitivo entra no relatório como está**, com análise.

A dimensão "Testes e Validação" avalia *"plano de testes, execução e métricas
coletadas"* — não avalia se os números foram bonitos. Um número honesto que exige
explicação demonstra mais domínio técnico que um número redondo sem lastro. E um relatório
onde todos os critérios passam de primeira, num sistema distribuído, é mais suspeito que
convincente.
