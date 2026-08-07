# Trade-offs

Toda decisão de projeto tem um custo. Este documento registra o que aceitamos pagar
e o que ficou de fora — inclusive as limitações que ainda estão de pé.

O critério para entrar aqui: **coisas que alguém poderia razoavelmente ter feito
diferente**. Não é uma lista de defeitos, é o inventário das escolhas que têm dois
lados.

---

## 1. O algoritmo do token bucket existe duas vezes

**O que fizemos.** O token bucket está implementado em Python
(`resilience/token_bucket.py`) **e** em Lua (`lua/token_bucket.lua`). O mesmo vale
para a máquina de estados do circuit breaker.

**O que pagamos.** Duas fontes de verdade para o mesmo algoritmo. Alguém pode
corrigir um bug numa e esquecer a outra.

**Por que aceitamos.** A versão Lua é obrigatória — sem execução atômica dentro do
Redis, cinco workers podem ler "resta 1 ficha" simultaneamente e cinco requisições
saem ([ADR-005](adr/ADR-005-script-lua-para-atomicidade.md)). A versão Python entrega
três coisas que a Lua não entrega:

1. **Testabilidade** — 17 casos de borda (relógio para trás, refill fracionário,
   pedido maior que a capacidade) rodando em milissegundos, sem Docker.
2. **Legibilidade** — é a *explicação* do algoritmo. Quem quer entender o mecanismo lê
   Python.
3. **Referência para verificação** — permite o teste de paridade.

**Como mitigamos.**
`tests/integration/test_rate_limiter_redis.py::TestParidade` roda 30 passos da mesma
sequência nas duas implementações e compara `allowed`, `tokens_remaining` e
`retry_after_ms`. Se alguém alterar uma e esquecer a outra, o teste aponta o passo
exato da divergência.

**Alternativa rejeitada.** Manter apenas o Lua e testá-lo só contra o Redis. Perderíamos
os testes rápidos de borda e a explicação legível — e o CI passaria a exigir Redis para
qualquer verificação do algoritmo central.

---

## 2. Fail-open quando o Redis cai

**O que fizemos.** Se o Redis estiver inacessível, o rate limiter e o circuit breaker
**permitem** o envio, logam em nível ERROR e seguem.

**O que pagamos.** Durante uma queda de Redis, a vazão pode exceder o limite da
plataforma. É a decisão mais discutível do projeto.

**Por que aceitamos.** A alternativa (fail-closed) transformaria uma queda de Redis em
**indisponibilidade total**: nenhum envio sairia, as tarefas acumulariam na fila até
estourar o `x-max-length` e começariam a ser descartadas.

E o fail-open é menos arriscado do que parece isoladamente, porque as outras camadas
continuam ativas:

| Camada | Continua funcionando sem Redis? |
|---|---|
| Bulkhead (semáforo local) | **sim** — limita a concorrência por plataforma |
| Retry com backoff | **sim** — espaça as tentativas |
| Timeout de envio | **sim** |
| 429 da plataforma | **sim** — ainda chega e ainda é tratado |

Perdemos a *antecipação*, não todas as defesas.

**Como mitigamos.** Log em ERROR com nota explícita (`rate_limiter.unavailable_fail_open`)
e `appendonly yes` no Redis, para que os buckets sobrevivam a um restart e não liberem
uma rajada inteira ao voltar.

**O que faria a decisão mudar.** Num sistema onde exceder o limite causa banimento
permanente (e não throttling temporário), fail-closed seria a escolha correta. É uma
decisão de domínio, não técnica.

---

## 3. Semântica at-least-once permite envio duplicado

**O que fizemos.** O worker publica o retry **antes** de dar ack na mensagem original.
Se o processo morrer entre as duas operações, o broker reentrega a original e a tarefa
é processada duas vezes.

**O que pagamos.** A plataforma pode receber o mesmo envio duas vezes.

**Por que aceitamos.** A ordem inversa (ack primeiro) **perderia** a tarefa se a
publicação falhasse. Duplicar é recuperável; perder não é.

**O que ficou de fora — e é a limitação mais séria do sistema.** Idempotência ponta a
ponta: uma chave de idempotência por tarefa, verificada antes do envio, de forma que um
reprocessamento não gere segundo envio. Ficou fora do escopo ([ADR-011](adr/ADR-011-reducao-de-escopo-dos-padroes.md))
— idempotência é um dos padrões recomendados da **POC 3**, não da 4.

Registramos isso como limitação conhecida e não como algo resolvido. Num sistema real
de engajamento, envio duplicado é visível para o usuário final.

---

## 4. Contadores desnormalizados em `campaigns`

**O que fizemos.** `dispatched_count`, `sent_count` e `failed_count` são mantidos na
tabela `campaigns`, além de serem deriváveis de `send_tasks`.

**O que pagamos.** Dois lugares com a mesma informação. Se a aplicação errar a
atualização, os números divergem — e o valor "errado" é o que o usuário vê.

**Por que aceitamos.** O dispatcher precisa de `dispatched_count` **a cada tick** para
saber quanto do orçamento resta. Um `COUNT(*)` em `send_tasks` por campanha ativa, a
cada segundo, com centenas de milhares de linhas, seria o gargalo do scheduler.

**Como mitigamos.** As atualizações acontecem na **mesma transação** que a escrita que
elas contam (`register_dispatch` incrementa e completa a campanha num único `UPDATE`
com `CASE`), então não há janela onde o contador esteja errado.

**Alternativa rejeitada.** Uma *materialized view* com refresh periódico. Recusada por
introduzir atraso justamente no número que o dispatcher usa para decidir.

---

## 5. Backoff em degraus fixos, não contínuo

**O que fizemos.** Três filas de retry com TTL fixo (1s, 5s, 30s). O atraso calculado é
arredondado **para cima** até o próximo degrau.

**O que pagamos.** Um backoff de 6s espera 30s. O jitter fino é perdido — sobra apenas
a escolha do degrau.

**Por que aceitamos.** A alternativa (TTL por mensagem numa fila única) sofre de
*head-of-line blocking*: no RabbitMQ, a expiração só é avaliada quando a mensagem chega
à **cabeça** da fila. Uma mensagem com TTL de 30s publicada antes de outra com TTL de 1s
bloqueia a segunda pelos 30 segundos inteiros. O bug seria intermitente e dificílimo de
diagnosticar.

**Alternativa rejeitada.** O plugin `rabbitmq_delayed_message_exchange` resolve
elegantemente — atraso arbitrário por mensagem, sem head-of-line blocking. Recusado por
exigir habilitação de plugin de terceiros na imagem do broker, para um ganho que os três
degraus já cobrem.

Detalhes em [ADR-009](adr/ADR-009-retry-com-filas-ttl.md).

---

## 6. `PATCH /platforms/{platform}` não afeta os workers em execução

**O que fizemos.** O endpoint grava o novo `allowed_rps` no Postgres. Os workers,
porém, leem os parâmetros do bucket do **próprio `.env`** (`config.RateLimitConfig`).

**O que pagamos.** O endpoint altera o valor de referência e a documentação viva do
sistema, mas **não muda o comportamento** dos workers rodando. É uma inconsistência
real.

**Por que está assim.** Fechar o ciclo exigiria os workers lerem o threshold do Redis
(com o Postgres como fonte de verdade) e invalidarem cache por evento de fanout — que é
exatamente o mecanismo que **já existe** nas feature flags. Ficou fora por tempo, não
por dificuldade.

**Como mitigamos.** A limitação está declarada em três lugares: no docstring do endpoint,
no log de WARNING que ele emite (`note="workers em execução seguem usando os valores do
.env"`) e aqui. Optamos por deixá-la explícita em vez de escondê-la — um endpoint que
parece funcionar e não funciona é pior que um endpoint ausente.

**Como resolver.** Reutilizar o padrão de `feature_flags.py`: ler do Redis com cache de
2s, invalidar por `flags_changed`. Cerca de 40 linhas.

---

## 7. Uma imagem Docker para três processos

**O que fizemos.** API, worker e simulador usam a **mesma imagem**, mudando apenas o
`command`.

**O que pagamos.** A imagem do worker carrega o FastAPI, que ele não usa. Alguns MB
desnecessários por container.

**Por que aceitamos.** Um build para manter em cache em vez de três, e — o que importa
mais — garantia de que API e worker rodam **exatamente o mesmo código de domínio**. Com
três builds, existiria a possibilidade de versões divergentes entre serviços.

---

## 8. SQL explícito sem verificação de tipos

**O que fizemos.** SQLAlchemy Core com `text()`, sem ORM. As linhas voltam como
`dict[str, Any]`.

**O que pagamos.** O mypy não verifica os tipos das colunas. Um nome de coluna escrito
errado só falha em runtime. E `NUMERIC` volta como `Decimal`, exigindo conversão manual
feia (`float(str(row["allowed_rps"]))`).

**Por que aceitamos.** As consultas que importam usam recursos específicos do Postgres
(`SKIP LOCKED`, `ON CONFLICT`, `percentile_cont`) que, no ORM, virariam
`session.execute(text(...))` de qualquer forma — e manteríamos modelos declarativos
duplicando o schema.

**Como mitigamos.** Todo o SQL num único arquivo, e os testes de integração exercitam
cada consulta contra o Postgres real.

Detalhes em [ADR-012](adr/ADR-012-sql-puro-em-vez-de-orm.md).

---

## 9. Migrações só na primeira inicialização do volume

**O que fizemos.** `db/migrations/001_init.sql` roda via
`/docker-entrypoint-initdb.d` do Postgres.

**O que pagamos.** É a limitação mais séria da camada de dados: **alterar o schema exige
`docker compose down -v`, o que apaga os dados**.

**Por que aceitamos.** A POC tem uma migração inicial e nenhuma evolução prevista.
Alembic exigiria diretório de versões, configuração e ambiente — e, sem modelos
declarativos, a autogeração não funcionaria (escreveríamos o SQL na mão dentro dos
arquivos de migração de qualquer forma).

**Isto não é um argumento de que Alembic é desnecessário.** Em qualquer contexto com
dados que importam, ele entraria antes da primeira alteração de schema.

---

## 10. Endpoints administrativos sem autenticação

**O que fizemos.** `/admin/*` e `/flags/*` são públicos.

**O que pagamos.** Num sistema real seria uma falha grave: qualquer um poderia chamar
`POST /admin/reset/rate-limiter` e liberar uma rajada, ou desligar o rate limiter por
`PATCH /flags/rate_limiter_enabled`.

**Por que está assim.** A POC roda localmente, com portas expostas apenas para a máquina
de desenvolvimento, e autenticação (OAuth2/JWT) ficou fora do escopo acordado. Está
declarado no docstring de `api/routers/admin.py` — registramos a lacuna em vez de
fingi-la resolvida.

**Como resolver.** `HTTPBearer` do FastAPI + validação de JWT numa dependência aplicada
aos routers `admin` e `flags`. Cerca de 30 linhas, sem tocar na lógica.

---

## 11. O bulkhead é local a cada worker

**O que fizemos.** Os semáforos vivem na memória de cada processo. Cinco réplicas dão
5 × 8 = 40 slots totais de YouTube.

**O que pagamos.** A concorrência total contra uma plataforma cresce com o número de
réplicas.

**Por que aceitamos, e por que isso NÃO é o mesmo problema do rate limiter.** O bulkhead
limita recursos **deste processo** — corrotinas e conexões HTTP — e esses recursos são
locais por natureza. O limite que precisa de visão global é o de **vazão**, e esse sim é
distribuído. Um bulkhead distribuído seria um lock distribuído no caminho crítico,
resolvendo um problema que o rate limiter já resolve.

**O que fica de fato limitado pelos 40 slots:** apenas a concorrência instantânea. A
vazão continua barrada em 16 req/s pelo token bucket global.

---

## 12. Padrões e escopo removidos

Registrado em detalhe em [ADR-011](adr/ADR-011-reducao-de-escopo-dos-padroes.md).
Resumo do que ficou de fora e do custo:

| Removido | O que perdemos |
|---|---|
| **Traffic Sharding** | O eixo por conteúdo do rate limiter cria uma chave Redis por URL. Com milhões de URLs ativas, isso não escala (mitigado hoje por TTL de 1h). |
| **Fila inteligente (deficit round-robin)** | Sem evidência medida sobre *starvation* entre campanhas concorrentes. Aproximado pelo `ORDER BY updated_at ASC`. |
| **Idempotência** | Ver item 3 — é a limitação conhecida mais relevante. |
| **Terceira plataforma (TikTok)** | Nada: dois limites assimétricos já exercitam o bulkhead. |
| **Dashboard Grafana** | A instrumentação está completa (11 métricas); falta o painel visual. As consultas PromQL estão em [RESULTADOS-TESTES.md](RESULTADOS-TESTES.md). |

---

## 13. Os thresholds das plataformas são estimativas

**O que fizemos.** Atribuímos 5 req/s ao YouTube e 10 req/s ao Instagram — ver também o
[item 19](#19-calibrar-limites-em-funcao-da-capacidade-do-ambiente-de-teste-severidade-media-corrigido-nesta-entrega),
sobre por que o número do YouTube mudou de 20 para 5.

**O que pagamos.** A POC **não prova nada sobre as plataformas reais**. É a limitação
fundamental do trabalho.

**Por que é inevitável.** Descobrir os limites reais exigiria enviar volume de tráfego
artificial a APIs de terceiros — teste de carga não autorizado contra infraestrutura
alheia, e violação explícita dos termos de uso dessas plataformas. Não é uma opção num
trabalho acadêmico.

**O que a POC prova, então.** O **mecanismo** de respeitar um limite desconhecido com
margem de segurança, e de reagir corretamente quando o limite é atingido. Isso é
verificável, e é o que os testes medem.

**Como mitigamos.** A ressalva está no docstring de `domain/platforms.py`, na descrição
da API (`/docs`), no schema de `GET /platforms` e em
[ADR-008](adr/ADR-008-simulador-de-plataformas.md). Um número apresentado como oficial
quando é estimativa é uma afirmação falsa, mesmo com o mecanismo correto em volta.

---

## 14. Perda silenciosa de mensagens no retry (alta severidade, CORRIGIDO nesta entrega)

**O que fazíamos (antes da correção).** `publish_retry()` publicava a tarefa adiada
diretamente no exchange `apt.retry` (topic) com `routing_key=f"tier.{clamped}"`, para
escolher qual das três filas de espera (TTL 1s/5s/30s) ia recebê-la — compartilhadas
entre plataformas. Quando o TTL expirava, a fila de retry não definia
`x-dead-letter-routing-key`, então o RabbitMQ preservava a routing key que a mensagem
tinha **ao entrar nesta fila** — `tier.N` — e a redirecionava para o exchange
`apt.tasks` com essa mesma chave.

**O que pagávamos.** `apt.tasks` (topic) só tem bindings para `youtube` e `instagram`,
não para `tier.N`. A mensagem chegava inroteável e o RabbitMQ a descartava **em
silêncio** (comportamento padrão de exchange topic sem `mandatory`/`alternate-exchange`).
Efeito prático: **toda tarefa desviada por `_defer()` — rate limiter, bulkhead ou
circuit breaker — era perdida permanentemente ao expirar o TTL**, em vez de retornar
ao fluxo. A tarefa nunca chegava à DLQ nem à tabela `failures`: ela simplesmente
somia, sem deixar rastro algum.

**Causa raiz do erro de design.** Quem projetou a fila de retry raciocinou sobre a
routing key do *primeiro* publish da mensagem (a plataforma, usada quando o worker
originalmente a recebeu de `apt.tasks`). Mas o publish que importa para o
dead-lettering é o *mais recente* — o de `publish_retry`, que já substitui a routing
key por `tier.N` antes da mensagem entrar na fila de espera. O comentário original no
código descrevia esse raciocínio errado como se fosse o comportamento real.

**Confirmado antes de corrigir, não foi hipótese.** Publicando diretamente no exchange
`apt.retry` com `aio_pika`, fora do pytest, e observando via `rabbitmqctl list_queues`:
a mensagem aparecia na fila de retry imediatamente após o publish; passados 3s (TTL de
1s já expirado), ela não estava na fila de retry, não estava em `apt.tasks.youtube` e
não estava em `apt.dlq`. Ela desaparecia. O teste de integração
`test_messaging.py::TestRetryComTTL::test_retry_volta_para_a_fila_original` pegou
exatamente esse caminho e falhava — corretamente, porque o código é que estava errado.

**Impacto medido antes da correção.** Ver [RESULTADOS-TESTES.md](RESULTADOS-TESTES.md):
era a causa raiz (junto com o item 15, antes de ele ser corrigido) de A-3 (adiamentos
que deveriam ficar registrados como `rate_limited_local` simplesmente não retornavam)
e de B-3 (o circuito do Instagram nunca fechava, porque as tarefas adiadas durante a
janela `open` nunca voltavam para tentar de novo). No Cenário C, a taxa de conclusão
das campanhas **caía** conforme o número de workers subia (99,4% → 63,6% → 56,2% de
500 envios, com `pending=0` em todas — ou seja, toda tarefa foi tentada uma vez, e as
que foram adiadas ficaram permanentemente presas em `deferred`) — a mesma causa raiz,
amplificada pela maior concorrência pelo mesmo orçamento de tokens.

**Correção aplicada.**

```
apt.retry.youtube.1     TTL  1s   x-dead-letter-routing-key: youtube
apt.retry.youtube.2     TTL  5s   x-dead-letter-routing-key: youtube
apt.retry.youtube.3     TTL 30s   x-dead-letter-routing-key: youtube
apt.retry.instagram.1   TTL  1s   x-dead-letter-routing-key: instagram
apt.retry.instagram.2   TTL  5s   x-dead-letter-routing-key: instagram
apt.retry.instagram.3   TTL 30s   x-dead-letter-routing-key: instagram
```

Seis filas (`topology.py`), uma por combinação plataforma×degrau, com binding
`tier.N.<plataforma>` no exchange `apt.retry` e `x-dead-letter-routing-key` declarado
como a própria plataforma — não mais "preservado" da entrada. `publish_retry`
(`publisher.py`) passa a publicar com essa routing key composta, obtida de
`message.platform`. `Topology.retry_queues` mudou de `dict[int, AbstractQueue]` para
`dict[tuple[str, int], AbstractQueue]` — refletido em
`tests/integration/test_messaging.py`, que precisou indexar pela nova chave.

Efeito colateral positivo, como previsto: os retries agora são isolados por plataforma
também na fila de espera, reforçando o padrão Bulkhead (antes, um retry do Instagram e
um do YouTube competiam pela mesma fila `apt.retry.N`).

**Por que foi corrigido nesta rodada, e não na anterior.** A primeira avaliação deste
bug ("só importa se a demo mostrar a DLQ") valia enquanto o item 15 (vazamento de
sonda) ainda não estava corrigido: o circuito ficava travado em `half_open`,
interceptando tarefas na Camada 3 antes de chegarem ao rate limiter — e A-3 registrava
zero adiamentos, então este bug nunca tinha a chance de se manifestar. Corrigido o
item 15, o caminho de adiamento se abriu, e é exatamente esse caminho que este bug
destrói: numa demonstração sobre rate limiting, uma fração da campanha ficaria
travada para sempre. **Um bug que só se manifesta depois de outro ser corrigido é
acoplamento real entre os dois** — vale a pena registrar isso na apresentação.

---

## 15. Vazamento de sonda no half_open do circuit breaker (alta severidade, CORRIGIDO nesta entrega)

**O que fizemos (antes da correção).** Em `half_open`, `probes_in_flight` era
incrementado quando o breaker admitia uma sonda (`allow`) e só decrementado nos
branches `success`/`failure` do script Lua — chamados por `record_success`/
`record_failure`, que só acontecem depois de uma tentativa real de envio (Camada 5).

**O que pagamos.** Uma sonda admitida pelo breaker (Camada 3) pode ser desviada por
uma camada **posterior** — o rate limiter (Camada 4) — antes de qualquer envio
acontecer. Nesse caso o worker chama `_defer()` e retorna sem nunca chamar
`record_success` nem `record_failure`: o slot da sonda fica ocupado para sempre. Após
`half_open_probes` (2) vazamentos como esse, nenhuma sonda nova é admitida,
`success_count` nunca alcança `success_threshold`, e o circuito **trava em half_open
permanentemente** — só saindo se o registro expirar do Redis (TTL de 600s) ou alguém
resetar manualmente. Foi exatamente o que produziu a falha do critério B-3
(`RESULTADOS-TESTES.md`): o circuito do Instagram abriu, sondou, e nunca fechou dentro
da janela de observação.

**A ironia que vale registrar.** A ordem breaker-antes-do-limiter (Camada 3 antes da
Camada 4) foi uma decisão deliberada e documentada: o breaker é mais barato (1 ida ao
Redis vs. 2) e evita gastar ficha do rate limiter com um envio que já sabemos que não
vai sair se a plataforma está fora do ar. É **exatamente essa ordem** que cria a
condição do vazamento — uma sonda só pode ser "admitida e depois recusada por outra
camada" porque existe uma camada depois dela. Otimizar o caminho comum (barato antes
do caro) abriu uma janela no caminho raro (sonda de recuperação) que ninguém tinha
testado, porque half_open só aparece sob falha real e concorrência com outras camadas —
exatamente a combinação que só apareceu ao rodar os testes de carga/resiliência de
ponta a ponta.

**Correção aplicada.**

- `resilience/lua/circuit_breaker.lua`: nova operação `release`, que decrementa
  `probes` em `half_open` sem tocar em `failures`/`successes` (não houve resposta da
  plataforma para registrar — só a sonda que não aconteceu).
- `resilience/breaker_state.py`: `evaluate_release(snapshot)`, a implementação pura de
  referência espelhando a mesma lógica, com teste de paridade conceitual (mesma
  disciplina do item 1 deste documento).
- `resilience/circuit_breaker.py`: `CircuitBreaker.release_probe(platform)`, fachada
  com o mesmo tratamento de erro (fail-open, log em WARNING) dos demais métodos.
- `worker/main.py`: `handle_task` registra se a Camada 3 admitiu a tarefa como sonda
  (`decision.state is BreakerState.HALF_OPEN`) e, se a Camada 4 negar o envio, chama
  `release_probe` antes de adiar a tarefa.
- `tests/unit/test_breaker_state.py`: dois testes novos — a sonda devolvida libera o
  slot para uma nova sonda, e `release` fora de `half_open` é no-op.

---

## 16. Calibração do burst: a invariante testada era insuficiente (severidade média, CORRIGIDO nesta entrega)

**O que fizemos (antes da correção).** `burst_capacity` era 16 para o YouTube e 8 para
o Instagram — os mesmos valores de `allowed_rps`. O único teste que verificava essa
relação (`test_burst_nao_passa_do_limite_estimado`) checava apenas
`burst_capacity <= estimated_limit_rps` (16 ≤ 20, passava).

**O que pagamos.** Essa invariante ignora o refill. No pior caso — bucket cheio mais o
refill do mesmo segundo — uma única janela de 1s do simulador
(`PlatformThrottle._evict_expired`, janela deslizante de 1s) via até
`burst_capacity + allowed_rps` requisições: 16+16=32 no YouTube, onde o limite é 20.
**O pior caso estourava o limite por construção**, não por desalinhamento de relógio
entre a nossa janela e a do simulador. Foi a causa raiz confirmada (dados agrupados no
início de cada campanha, consistentes com uma rajada de bucket cheio) dos 429 reais
observados mesmo com o rate limiter ligado, na primeira execução dos testes de carga
(ver RESULTADOS-TESTES.md).

**Correção aplicada.** A invariante correta soma os dois termos:

```
burst_capacity + allowed_rps <= estimated_limit_rps
```

Com essa fórmula, o teto exato é `burst<=4` (YouTube: 4+16=20) e `burst<=2` (Instagram:
2+8=10). Os valores escolhidos — **3** e **1** — ficam um degrau abaixo do teto exato,
não nele: no teto exato o sistema fica na fronteira, e qualquer desalinhamento real
entre as duas janelas devolveria 429 de qualquer forma, inclusive durante uma
demonstração ao vivo. Alterado em `.env.example`, `domain/platforms.py`,
`db/migrations/001_init.sql` (seed) e `tests/unit/test_domain.py` (teste renomeado para
`test_burst_mais_refill_nao_passa_do_limite_estimado`, com o docstring explicando por
que a versão anterior era insuficiente).

**Por que a lição importa mais que o número.** O bug não era a lógica do token bucket
(coberta a 100% nos testes unitários, ver `token_bucket.py`) — era a calibração dos
*parâmetros de entrada* dela, verificada por uma invariante estruturalmente incompleta
que só olhava a vazão sustentada. Um teste unitário que passa não prova que os valores
testados fazem sentido; prova que eles satisfazem a propriedade que o teste verifica. Se
a propriedade estiver incompleta, o teste passa e o bug atravessa para produção — que
foi exatamente o que aconteceu aqui, e só apareceu ao medir contra a infraestrutura real.

---

## 17. Métricas administrativas sem filtro de campanha (baixa severidade, afeta medição)

**O que fizemos.** `ExecutionRepository.outcome_breakdown()` e `.latency_percentiles()`
filtram por `platform` (opcional); `ExecutionRepository.worker_distribution()` não
filtra nada. Nenhum dos três aceita `campaign_id` ou uma janela de tempo.
`GET /campaigns/{id}/status` chama `outcome_breakdown(conn)` **sem nenhum filtro** —
promete o progresso de uma campanha específica e devolve a soma de toda a tabela
`executions` desde que o banco foi criado (ou desde o último truncate).

**O que pagamos.** Em qualquer execução com mais de uma campanha na mesma base (o caso
normal de `scale_test.py`, que roda três cenários — 1, 3 e 5 workers — em sequência, no
mesmo processo, sem truncar o Postgres entre eles), os números de `/admin/outcomes` e
`/admin/workers` do cenário N incluem os totais de todos os cenários anteriores. Foi
exatamente isso que produziu os falsos "FALHOU" nos critérios C-2 e C-4 na primeira
rodada de medição — corrigido na análise isolando por `campaign_id` diretamente no
Postgres, não no código (ver RESULTADOS-TESTES.md).

**Por que não contamina C-1 e C-3.** As duas métricas que sustentam a tese central do
projeto — `peak_rps`, do simulador — vêm de `GET /admin/stats`, que é resetado
explicitamente pelo `reset_all()` **a cada cenário**. O artefato de acumulação afeta
apenas os números que **nós** registramos sobre nós mesmos; não afeta o que a
plataforma (o simulador) observou de fora. É por isso que a Nota de Método em
RESULTADOS-TESTES.md recomenda tratar `peak_rps` como a evidência forte e os endpoints
`/admin/outcomes`/`/admin/workers` como diagnóstico secundário quando mais de uma
campanha existe na base.

**Por que não corrigimos o código.** O ajuste é pequeno (um parâmetro `campaign_id`
opcional nos dois métodos de repositório e nos endpoints que os expõem), mas nenhuma das
três demonstrações do roteiro ao vivo depende desses dois endpoints para o número
principal — a Demo 1 usa `/admin/stats`, que já é isolado por cenário. Preferimos ajustar
o roteiro (usar `/admin/stats` como número principal, mencionar a ressalva ao mostrar
`/admin/workers`) a arriscar uma mudança de assinatura de API na véspera da
apresentação.

**Correção proposta.** `outcome_breakdown(conn, *, platform=None, campaign_id=None)`,
`latency_percentiles(conn, *, platform=None, campaign_id=None)` e
`worker_distribution(conn, *, campaign_id=None)`, com `WHERE (:campaign_id IS NULL OR
campaign_id = CAST(:campaign_id AS UUID))` — usando o `CAST` nas duas ocorrências do
parâmetro, não só na comparação (ver o bug de `AmbiguousParameterError` documentado em
RESULTADOS-TESTES.md § 1.5.1, que é exatamente essa classe de erro).

---

## 18. Publish dentro da transação aberta do dispatcher (alta severidade, CORRIGIDO nesta entrega)

**O que fizemos (antes da correção).** `Dispatcher.tick()` abre uma única transação
(`async with connection()`) para todas as campanhas ativas de um tick (até 20) e,
dentro dela, `_dispatch_campaign()` chamava `publisher.publish_task()` para **cada**
tarefa, imediatamente após o `INSERT` em `send_tasks` — mas ainda dentro da mesma
transação, que só commita quando o bloco inteiro termina. Com
`APT_DISPATCH_MAX_BATCH=200`, um único tick materializa e publica até 200 mensagens
antes do commit.

**O que pagamos.** Um worker local, rápido o suficiente, podia consumir a **primeira**
mensagem publicada e tentar `INSERT INTO executions` (com FK para `send_tasks`) antes
de a transação que criou aquela linha ter comitado — Postgres usa MVCC, e uma linha
inserida numa transação ainda aberta não é visível para outra conexão, mesmo que o
`INSERT` já tenha "acontecido" no tempo. Resultado: `asyncpg.exceptions.
ForeignKeyViolationError`, tratado por `consumer.py` como falha terminal
(`nack(requeue=False)`, direto para a DLQ) — **mesmo quando o envio HTTP à
plataforma já tinha sido concluído com sucesso** (o erro acontecia ao *registrar* o
resultado, não ao enviá-lo). A tarefa desaparece do nosso registro como se tivesse
falhado, quando na verdade só a contabilidade falhou.

**Como foi descoberto.** Não estava nos três bugs conhecidos no início desta rodada.
Apareceu ao investigar por que o Cenário B (circuito do Instagram) não abria após a
correção do vazamento de sonda (item 15): `_record_execution()` — chamado ANTES de
`record_success`/`record_failure` no worker — lançava essa exceção e interrompia
`_handle_result()` antes de o breaker ser notificado, mascarando o resultado real
(inclusive falhas genuínas da plataforma durante a falha injetada).

**Confirmado como bug geral, não específico deste projeto de teste.** Reproduzido de
forma isolada: uma campanha de 400 envios criada diretamente por `curl`, sem nenhum
truncate/purge do meu próprio harness de teste por perto, produziu 30
`ForeignKeyViolationError` em 209 tentativas (~14%). Uma campanha de 30 envios não
reproduziu — o bug só se manifesta em lotes grandes o suficiente para a janela entre o
primeiro `INSERT` do tick e o commit final ficar exposta a um consumidor rápido.

**Correção aplicada.** `_dispatch_campaign()` deixou de publicar — agora coleta os
`SendTaskMessage` de cada tarefa materializada e os devolve para `tick()`.
`tick()` publica todos eles **depois** que o bloco `async with connection()` sai (ou
seja, depois do commit). A ordem "grava no banco antes de publicar" continua valendo
(ver item no docstring do módulo) — a correção garante que "antes de publicar" também
signifique "depois de comitado", não apenas "depois do INSERT dentro da transação
ainda aberta". Verificado com o mesmo teste isolado de 400 envios: 0
`ForeignKeyViolationError` em 103 tentativas, após a correção.

---

## 19. Calibrar limites em função da capacidade do ambiente de teste (severidade média, CORRIGIDO nesta entrega)

**O que fizemos (antes da correção).** O YouTube estava calibrado em `allowed_rps=16`
(`estimated_limit_rps=20`). Essa VM compartilhada tem um teto de vazão **agregada**
medido em ~6-8 req/s (ver RESULTADOS-TESTES.md § 1.6) — abaixo do que o rate limiter do
YouTube jamais chegaria a restringir.

**O que pagávamos.** O teto de hardware do ambiente de teste virou um **confundidor da
hipótese central da POC**. No teste de escala (C-3), o platô observado nunca chegava
perto de 16 req/s, então não havia como saber se o platô media o **mecanismo** (o rate
limiter) ou o **ambiente** (o teto da VM) — as duas explicações eram igualmente
compatíveis com o mesmo número, e nenhuma medição as separava. No teste de carga (A-4),
o contrafactual (desligar a flag) não produzia 429 nem com 5 workers: a demanda nunca
superava o teto do ambiente, que já ficava abaixo do limite do simulador (20). Ou seja,
o contrafactual media o teto do consumidor, não o do rate limiter — o controle do
experimento estava quebrado.

**Por que isso é diferente de "o ambiente é lento".** Um teto de ambiente lento não é
problema em si — só é um confundidor quando fica **próximo ou abaixo** do parâmetro que
o mecanismo deveria testar. Com `allowed_rps=16` contra um teto de ~6-8, o ambiente
sempre "vencia" primeiro; o mecanismo nunca era genuinamente exercitado sob a carga que
o teste dizia estar aplicando.

**Correção aplicada.** Recalibrado **somente o YouTube**: `allowed_rps` 16→3,
`burst_capacity` 3→1, `estimated_limit_rps` 20→5 — todos abaixo do teto medido do
ambiente (~6-8 req/s). Quatro lugares precisaram mudar em conjunto (`.env`/`.env.example`,
`src/apt/domain/platforms.py`, o seed de `platform_thresholds` em
`db/migrations/001_init.sql`, e o próprio limite do simulador, que lê
`estimated_limit_rps` do mesmo perfil de domínio — ver `platform_sim/main.py`).
Divergir entre eles reproduziria exatamente a falha que o
[item 6](#6-patch-platformsplatform-nao-afeta-os-workers-em-execucao) já descreve para
o `PATCH /platforms`: a tela mostraria um número e o runtime aplicaria outro.

**O Instagram não mudou.** Seu `allowed_rps` (8) já ficava dentro do mesmo patamar do
teto do ambiente, então o confundidor nunca se manifestou nele — recalibrá-lo invalidaria
o Cenário B (resiliência/bulkhead), já validado, sem necessidade.

**Custo assumido.** Os números do YouTube deixam de ter qualquer relação, mesmo
estimada, com um limite real de plataforma — ver item 13. Antes disso já era verdade
que os números eram estimativas; agora são estimativas escolhidas em função da
capacidade do *harness*, não da plataforma. É uma segunda camada de "isto não prova
nada sobre o YouTube real", empilhada sobre a que já existia.

**Benefício, e a assimetria que passou a existir de propósito.** O experimento volta a
medir o que afirma medir: o platô do C-3 agora coincide com o número configurado (3), não
com um teto de hardware não intencional, e o contrafactual do A-4 volta a produzir 429 de
verdade. Como efeito colateral aceito, as duas plataformas passam a exercitar regimes
diferentes no mesmo ambiente: o YouTube fica limitado pelo **mecanismo** (bem abaixo do
teto do ambiente), o Instagram continua limitado pelo **próprio número configurado**, que
por coincidência fica no mesmo patamar do teto do ambiente. Vale registrar isso como
intencional, não como inconsistência.

**Como validamos.** Ver RESULTADOS-TESTES.md §§ 1.6-1.8 para os números de A-4 e C-3
depois da recalibração, e a confirmação de que nenhuma configuração excedeu o limite do
simulador.

---

## 20. O critério relativo do C-3 contradizia uma invariante do próprio projeto (severidade média, CORRIGIDO nesta entrega)

**O que fazíamos (antes desta correção).** Depois da recalibração do item 19, o
critério unilateral do C-3 (corrigido na rodada anterior desta mesma entrega) comparava
o pico de cada configuração contra a linha de base (1 worker) com tolerância de
`1.15×`. Medido: pico de 3 req/s com 1 worker e 4 req/s com 5 workers — crescimento de
33%, reprovado pela tolerância de 15%.

**O que isso expôs.** Não era a medição que estava errada — era o critério. Este
mesmo projeto declara, em `test_domain.py::
test_burst_mais_refill_nao_passa_do_limite_estimado`, que
`burst_capacity + allowed_rps ≤ estimated_limit_rps` é a invariante correta de
calibração. Para o YouTube recalibrado (item 19), isso autoriza explicitamente
`1 + 3 = 4` req/s numa única janela do simulador. Um teto relativo de `1.15×` sobre uma
linha de base de 3 req/s é `3.45` — **e reprova o próprio `4` que a especificação do
burst permite**. Duas partes do mesmo projeto afirmavam coisas incompatíveis: uma que
`4` é um resultado legítimo e esperado, outra que `4` é uma falha. A causa estrutural:
o menor incremento inteiro possível (1 req/s) já é 33% de um baseline de 3 req/s, mas
era só 6% do baseline de 16 req/s para o qual a tolerância de 15% tinha sido
calibrada — a razão relativa **não escala** para baselines pequenos.

**Por que não bastava alargar a tolerância.** Subir o número até `1.33×` (ou mais)
caber seria exatamente "ajustar a tolerância até o teste passar", que a política de
reporte deste projeto proíbe — e não corrigiria a incompatibilidade de fundo entre o
critério relativo e a invariante algébrica do burst; só esconderia o próximo caso em
que elas colidirem.

**Correção aplicada.** Substituído o critério relativo por medição direta contra os
dois tetos que a hipótese central realmente afirma: o pico não excede o teto algébrico
do bucket compartilhado (`peak_rps ≤ allowed_rps + burst_capacity`, em toda
configuração — este teto **não** cresce com o número de workers, porque o estado é
compartilhado) e não excede o limite da plataforma (já coberto por C-1). Um rate
limiter em memória de processo violaria o primeiro teto exatamente ao escalar — 5
processos, 5 baldes, `5 × 4 = 20` req/s possíveis contra o `4` medido — e essa
violação, não uma razão percentual, é o que o critério agora detecta. A razão relativa
contra a linha de base continua disponível no script como informação (fora do
veredito), aplicável só quando a linha de base for `≥ 20 req/s` — o baseline para o
qual `1.15×` tinha sentido original.

**Por que esta é a segunda correção de critério do projeto, e as duas precisam ficar
defensáveis lado a lado.** A primeira (rodada anterior) trocou uma banda bilateral por
unilateral, porque a hipótese só é falsificável por crescimento. Esta trocou um proxy
relativo — que funcionava no baseline antigo e quebrou silenciosamente no novo — por
medição direta contra a própria especificação do mecanismo. As duas mudanças têm a
mesma forma: identificar que o **critério**, não o sistema, não correspondia à
hipótese, e corrigir com o raciocínio explícito registrado — nunca girando um número de
tolerância até o resultado desejado aparecer.

**Como validamos.** `peak_rps` de 3/3/4 req/s em 1/3/5 workers, contra um teto algébrico
de `1 + 3 = 4` — dentro do teto em toda configuração. Ver RESULTADOS-TESTES.md § 1.8.

---

## 21. `jitter_strategy=humanized` é realista, mas inadequado para medição e demonstração reprodutíveis (severidade média, CORRIGIDO nos scripts de teste e nas demos)

**O que fazíamos.** `create_campaign()` (harness de teste) e os exemplos de campanha do
README/ROTEIRO usavam o padrão da API, `jitter_strategy=humanized` — a estratégia que
modula a demanda pelo perfil de atividade da **hora do dia**
(`jitter.py::HOURLY_ACTIVITY_PROFILE`, multiplicador de 0.12 às 3h-4h UTC a 1.50 às
19h-20h UTC).

**O que isso custou.** Foi um confundidor real, não hipotético: a primeira execução de
`load_test.py` depois da recalibração do YouTube (item 19) mostrou pico de ~2 req/s em
**ambos** os cenários — com e sem proteção — abaixo até do próprio `allowed_rps` de 3.
A calibração já estava correta (confirmada via `GET /platforms` e `/admin/stats` antes
de investigar qualquer outra coisa); a causa era a hora da execução (~05h UTC, dentro
da janela de multiplicador 0.12-0.25) suprimindo a demanda real bem abaixo do
`target_rate_per_min` solicitado. Um teste de carga controlado, ou uma demonstração ao
vivo, não pode ter seu resultado dependente do relógio de parede — o professor rodando
o mesmo `curl` às 4h da manhã veria um sistema aparentemente ocioso; às 20h, veria
volume 50% acima do esperado.

**Por que `humanized` continua existindo, e não foi removido.** É o comportamento
correto para o caso de uso que motivou o padrão de `create_campaign()` em primeiro
lugar: campanhas de produção reais têm ritmo humano, com picos e vales previsíveis ao
longo do dia — é exatamente esse realismo que o padrão da API existe para oferecer, e
os testes unitários de `jitter.py` (`test_humanized_reduz_o_volume_de_madrugada`, entre
outros) continuam validando esse comportamento como correto. O defeito não estava no
mecanismo, estava em usá-lo onde a demanda precisa ser **controlada**, não realista.

**Correção aplicada.** `tests/load/load_test.py` e `tests/load/scale_test.py` passam
`jitter_strategy="uniform"` explicitamente em `create_campaign()`, ignorando a hora do
dia e mantendo a demanda proporcional só a `target_rate_per_min`. `examples/campaign.json`
e os comandos de campanha de demo em `README.md` e `docs/ROTEIRO-APRESENTACAO.md`
(Demos 1, 2 e 3) também passaram a usar `uniform` explicitamente, com uma linha
explicando o motivo em cada lugar — a mesma razão de reprodutibilidade vale para a
apresentação ao vivo quanto para os scripts automatizados.

**Como validamos.** Rodando `load_test.py` de novo com `uniform`: pico de 4 req/s
(protegido) e 5 req/s (desprotegido, com 133 respostas 429) — os números esperados
pela calibração, não mais dependentes da hora. Ver RESULTADOS-TESTES.md § 1.0 item 7 e
§ 1.6.

---

## Resumo: o que faríamos diferente com mais tempo

Em ordem de importância:

1. **Idempotência ponta a ponta** — fecha o único caminho por onde o sistema pode
   produzir efeito duplicado observável (item 3). É o item de maior impacto que
   permanece em aberto depois desta rodada.
2. **Filtro por `campaign_id` nas métricas administrativas (item 17)** — pequeno, mas
   necessário para as métricas de teste serem confiáveis sem análise manual por SQL.
   Teria evitado a confusão do C-4 desta rodada (§ 1.8 de RESULTADOS-TESTES.md).
3. **Fechar o ciclo do `PATCH /platforms`** — reutilizar o padrão das feature flags,
   ~40 linhas (item 6).
4. **Autenticação nos endpoints administrativos** — ~30 linhas (item 10).
5. **Alembic** — antes da primeira alteração de schema com dados reais (item 9).
6. **Dashboard Grafana** — a instrumentação já está pronta; falta o painel.
