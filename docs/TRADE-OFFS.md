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

**O que fizemos.** Atribuímos 20 req/s ao YouTube e 10 req/s ao Instagram.

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

## Resumo: o que faríamos diferente com mais tempo

Em ordem de importância:

1. **Idempotência ponta a ponta** — fecha o único caminho por onde o sistema pode
   produzir efeito duplicado observável (item 3).
2. **Fechar o ciclo do `PATCH /platforms`** — reutilizar o padrão das feature flags,
   ~40 linhas (item 6).
3. **Autenticação nos endpoints administrativos** — ~30 linhas (item 10).
4. **Alembic** — antes da primeira alteração de schema com dados reais (item 9).
5. **Dashboard Grafana** — a instrumentação já está pronta; falta o painel.
