# Código explicado

Documento de referência para a apresentação. Percorre o sistema arquivo por arquivo,
explicando **o que faz**, **por que assim** e — no fim de cada módulo — as **perguntas
que a banca provavelmente fará**, com a resposta.

A ordem segue a dependência: fundação → domínio → persistência → resiliência →
mensageria → scheduler → API → worker → simulador.

## Índice

1. [Fundação](#1-fundação) — `config.py`, `logging_setup.py`
2. [Domínio](#2-domínio) — `models.py`, `platforms.py`
3. [Persistência](#3-persistência) — `engine.py`, `repositories.py`, `001_init.sql`
4. [Resiliência](#4-resiliência----o-núcleo) — token bucket, breaker, bulkhead, retry, flags
5. [Mensageria](#5-mensageria) — topology, publisher, consumer
6. [Scheduler](#6-scheduler) — jitter, dispatcher
7. [API](#7-api) — schemas, deps, routers, main
8. [Worker](#8-worker) — sender, main
9. [Simulador](#9-simulador-de-plataformas) — throttle, main
10. [Observabilidade](#10-observabilidade) — metrics
11. [Q&A geral](#11-qa-geral) — as perguntas mais prováveis

---

## 1. Fundação

### `src/apt/config.py`

**O que faz.** Carrega toda a configuração de variáveis de ambiente com prefixo `APT_`,
usando `pydantic-settings`. Expõe `get_settings()` com cache.

**Por que assim.**

- **Um único ponto de leitura do ambiente.** Nenhum outro módulo chama `os.environ`. Isso
  garante que existe um lugar para descobrir o que é configurável, e que um nome escrito
  errado falha no boot com mensagem clara em vez de virar `None` no meio de um envio.
- **Subconfigurações separadas** (`RateLimitConfig`, `CircuitBreakerConfig`,
  `BulkheadConfig`), cada uma com o próprio prefixo de ambiente. Mantém `Settings`
  legível e agrupa o que é conceitualmente junto.
- **`@lru_cache` em `get_settings()`.** Evita reparsear o ambiente a cada requisição e
  garante que todos os módulos veem os mesmos valores.

**Trecho que merece atenção:**

```python
def for_platform(self, platform: Platform) -> tuple[float, int]:
    match platform:
        case Platform.YOUTUBE:   return self.youtube_rps, self.youtube_burst
        case Platform.INSTAGRAM: return self.instagram_rps, self.instagram_burst
    raise ValueError(f"plataforma sem configuracao de rate limit: {platform}")
```

O `raise` no final é inalcançável hoje (o enum é exaustivo). Está ali para o dia em que
alguém adicionar `TIKTOK` ao enum e esquecer de configurar o limite: **falha alto** em vez
de aplicar silenciosamente um limite errado.

### `src/apt/logging_setup.py`

**O que faz.** Configura o `structlog` e propaga um `correlation_id` automaticamente para
todos os logs do contexto.

**Por que assim.** Num sistema distribuído, um envio atravessa API → RabbitMQ → worker →
plataforma. Sem um identificador comum, reconstruir o caminho de uma tarefa significa
cruzar timestamps de três serviços na mão.

**A decisão técnica central:** `ContextVar`, não variável global.

```python
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")
```

Cada task do asyncio tem a sua própria cópia. Com uma variável global de módulo, dois
envios concorrentes no mesmo worker teriam os ids embaralhados — um bug que **só aparece
sob concorrência** e produz logs que apontam para a tarefa errada.

> ### Perguntas prováveis
>
> **"Por que não usar o `logging` padrão do Python?"**
> Usamos — o `structlog` está configurado por cima dele (`logging.basicConfig`), o que faz
> os logs do uvicorn, do SQLAlchemy e do aio_pika saírem no mesmo formato. O que o
> structlog acrescenta é log **estruturado**: cada evento é um dicionário com campos, não
> uma string formatada. Filtrar por `correlation_id=abc123` num agregador é trivial;
> extrair o mesmo de uma string com regex, não.
>
> **"O que acontece se o cliente não mandar `X-Correlation-ID`?"**
> `bind_correlation_id(None)` gera um novo id (12 caracteres hex). O middleware da API
> devolve o id no header da resposta, então quem chamou consegue correlacionar mesmo sem
> ter enviado um.

---

## 2. Domínio

### `src/apt/domain/models.py`

**O que faz.** Enums, o contrato da mensagem que viaja pelo RabbitMQ, e a classificação
de resultados. Puro — não importa banco, Redis nem HTTP.

**As duas coisas mais importantes deste arquivo:**

#### (a) `Outcome` separa rejeição externa de autolimitação

```python
@property
def is_platform_rejection(self) -> bool:
    """Somente estes alimentam o circuit breaker."""
    return self in {Outcome.THROTTLED, Outcome.ERROR, Outcome.TIMEOUT}

@property
def is_self_throttled(self) -> bool:
    return self in {Outcome.RATE_LIMITED_LOCAL, Outcome.CIRCUIT_OPEN, Outcome.BULKHEAD_FULL}
```

Se o adiamento do rate limiter contasse como falha da plataforma, **o rate limiter
funcionando corretamente abriria o circuit breaker** — e o sistema se autobloquearia sem
que a plataforma tivesse reclamado de nada. É o primeiro bug conceitual que aparece ao
juntar os dois padrões.

Há um teste garantindo que os dois grupos são **disjuntos**
(`test_os_dois_grupos_sao_disjuntos`).

#### (b) `SendTaskMessage` tem dois contadores

```python
attempt: int = 0   # falhas de ENVIO (a requisição saiu e foi recusada)
defers: int = 0    # ADIAMENTOS nossos (a requisição não saiu)
```

Num sistema saudável sob carga, os adiamentos são **frequentes e esperados**. Se
incrementassem `attempt`, uma tarefa adiada `max_attempts` vezes iria para a DLQ **sem
nunca ter sido enviada** — o sistema descartaria trabalho legítimo justamente quando
estivesse se protegendo corretamente.

**Detalhe de compatibilidade:** `from_dict` usa `coerce_int(data.get(...))` com default,
não `data["attempt"]`. Durante um deploy, mensagens publicadas pela versão anterior podem
estar na fila; estourar aqui mandaria todas elas para a DLQ.

### `src/apt/domain/platforms.py`

**O que faz.** Perfis das plataformas: limite estimado, `allowed_rps`, capacidade de
rajada, endpoint.

**A propriedade que explica a arquitetura:**

```python
@property
def safety_margin(self) -> float:
    return 1.0 - (self.allowed_rps / self.estimated_limit_rps)   # 0.20 = 20%
```

Três razões para não usar 100% do limite:

1. **Janelas desalinhadas.** A nossa contagem (token bucket) e a da plataforma (janela
   deslizante) não estão sincronizadas. Duas janelas desalinhadas podem, na virada, somar
   mais requisições do que qualquer uma delas mediu isoladamente.
2. **O limite é estimativa.** Se erramos para cima, a margem absorve.
3. **Retries consomem cota.** Sem folga, um retry legítimo já estoura.

> ### Perguntas prováveis
>
> **"Esses limites de 20 e 10 req/s são os limites reais do YouTube e do Instagram?"**
> Não, e isso está declarado no código, na API e no ADR-008. São estimativas para
> ambiente controlado. Descobrir os limites reais exigiria enviar tráfego artificial a
> APIs de terceiros — teste de carga não autorizado e violação dos termos de uso. O que a
> POC prova é o **mecanismo** de respeitar um limite desconhecido com margem, não os
> números.
>
> **"Por que `StrEnum` e não `Enum`?"**
> Porque esses valores atravessam várias fronteiras como texto: routing key do RabbitMQ,
> chave do Redis, coluna do Postgres, label do Prometheus. Com `StrEnum`,
> `Platform.YOUTUBE == "youtube"` é verdadeiro e não há `.value` espalhado pelo código.

---

## 3. Persistência

### `db/migrations/001_init.sql`

**O que faz.** 7 tabelas, 3 enums, índices e o seed das plataformas.

**A decisão de modelagem central:** separar `send_tasks` de `executions`.

```
send_tasks  -> uma linha por ENVIO      (estado final: sent / dead / ...)
executions  -> uma linha por TENTATIVA  (1 tarefa pode ter N execuções)
```

Sem essa separação, não daria para responder "quantas vezes tentamos?" sem perder o
estado final da tarefa. E é de `executions` que saem as latências p50/p95/p99 do
relatório.

**Índice parcial:**

```sql
CREATE INDEX campaigns_active_idx ON campaigns (platform, updated_at)
    WHERE status = 'active';
```

O dispatcher roda essa consulta a cada tick. O índice **parcial** mantém quente em cache
apenas o subconjunto `active`, que fica pequeno mesmo com muitas campanhas históricas.

### `src/apt/db/engine.py`

**Duas configurações do pool que merecem menção:**

```python
pool_pre_ping=True,   # SELECT 1 antes de entregar a conexão
pool_recycle=1800,    # recicla conexões de mais de 30 min
```

`pool_pre_ping` custa uma ida ao banco, e evita o erro clássico de pegar do pool uma
conexão que o Postgres já fechou — acontece **sempre** que se reinicia o container do
banco com o stack no ar.

### `src/apt/db/repositories.py`

**O que faz.** Todo o SQL do sistema. A regra: nenhum módulo fora de `apt.db` monta
consulta.

**Os métodos recebem a conexão como parâmetro**, em vez de abrir a própria transação.
Isso permite que quem chama componha várias escritas numa transação só — o dispatcher
cria a tarefa e incrementa o contador da campanha **atomicamente**.

#### Os dois trechos que a banca pode perguntar

**1. `claim_active_for_dispatch` — `FOR UPDATE SKIP LOCKED`**

```sql
SELECT ... FROM campaigns
WHERE status = 'active' AND dispatched_count < total_sends
ORDER BY updated_at ASC
LIMIT :limit
FOR UPDATE SKIP LOCKED
```

Se duas instâncias da API rodarem, cada dispatcher travaria as mesmas linhas e uma
ficaria bloqueada esperando — ou, pior, ambas materializariam as mesmas tarefas e a
campanha enviaria em dobro. Com `SKIP LOCKED`, a segunda **pula** as linhas travadas e
trabalha nas outras.

O `ORDER BY updated_at ASC` serve a campanha menos recentemente atendida primeiro: um
round-robin simples que impede uma campanha grande de monopolizar todos os ticks.

**2. `take_next` — rotação atômica do pool de URLs**

```sql
UPDATE campaign_contents
SET sends_count = sends_count + 1
WHERE id = (
    SELECT id FROM campaign_contents
    WHERE campaign_id = :cid
    ORDER BY (sends_count::numeric / weight) ASC, created_at ASC
    LIMIT 1 FOR UPDATE SKIP LOCKED
)
RETURNING id, content_url, weight, sends_count
```

Round-robin **ponderado suave**: escolhemos a URL com o menor `sends_count / weight`. Um
conteúdo de peso 2 acumula crédito na metade da velocidade, então recebe o dobro de
envios ao longo do tempo — sem manter índice de rodízio na aplicação.

*Por que não sortear aleatoriamente:* daria a distribuição certa **na média**, mas com
desvio visível em campanhas curtas. É perfeitamente possível sortear a mesma URL cinco
vezes seguidas — exatamente o padrão de concentração que queremos evitar.

*Por que `UPDATE ... RETURNING` num comando:* escolha e incremento são atômicos. Em dois
comandos, dois dispatchers concorrentes leriam a mesma URL antes de qualquer incremento.

> ### Perguntas prováveis
>
> **"Por que não usar ORM?"**
> As consultas que importam usam recursos específicos do Postgres (`SKIP LOCKED`,
> `ON CONFLICT`, `percentile_cont`) que, no ORM, virariam `session.execute(text(...))` de
> qualquer forma — e manteríamos modelos declarativos duplicando o schema. ADR-012.
> Reconhecemos o custo: perdemos verificação de tipos das colunas.
>
> **"Por que contadores desnormalizados em `campaigns`?"**
> O dispatcher precisa de `dispatched_count` a cada tick. Um `COUNT(*)` em `send_tasks`
> por campanha ativa, a cada segundo, seria o gargalo. A atualização acontece na **mesma
> transação** que a escrita que ela conta, então não há janela de inconsistência.

---

## 4. Resiliência — o núcleo

> Esta seção é a mais importante da apresentação. Quatro dos seis padrões vivem aqui.

### `src/apt/resilience/token_bucket.py` — o algoritmo puro

**O que faz.** Implementação de referência do token bucket: funções puras, sem I/O,
recebendo o tempo como parâmetro.

**O modelo mental, em uma frase.** Um balde tem `capacity` fichas e recebe `refill_rps`
fichas por segundo; cada requisição consome uma.

**A decisão de implementação que importa:** não há temporizador repondo fichas. Guardamos
`(tokens, updated_at)` e calculamos o refill **na leitura**:

```python
tokens_agora = min(capacity, tokens + (agora - updated_at) / 1000 * refill_rps)
```

Estado mínimo (dois números por balde), custo constante — o que importa porque esse
estado vive no Redis e é lido a cada envio.

**O trecho defensivo mais importante do arquivo:**

```python
elapsed_ms = max(0, now_ms - state.updated_at_ms)
```

Os workers passam `now_ms` do próprio relógio. Uma correção de NTP ou alguns
milissegundos de diferença entre containers podem produzir um `now_ms` **anterior** ao
`updated_at_ms` gravado por outro worker. Sem o clamp, `elapsed` negativo **removeria**
fichas — e o efeito seria um rate limiter intermitentemente mais restritivo que o
configurado, praticamente impossível de diagnosticar em produção.

**E o detalhe de justiça:**

```python
# Negado: o estado é gravado com as fichas ATUAIS (não consumidas)
return BucketDecision(allowed=False, ..., state=BucketState(tokens=available, ...))
```

Negar **não consome crédito**. Se consumisse, um cliente insistente empurraria o saldo
para negativo e atrasaria indefinidamente os outros.

### `src/apt/resilience/lua/token_bucket.lua` — a execução atômica

> **O arquivo mais importante do projeto.**

**O problema que resolve.** A versão ingênua faz:

```
tokens = GET bucket          -- (1) lê
if tokens >= 1 then
    SET bucket (tokens - 1)  -- (2) escreve
```

Entre (1) e (2) existe uma janela. Cinco workers podem ler "resta 1 ficha" ao mesmo
tempo, todos concluírem que podem enviar, e **cinco requisições saírem quando havia
orçamento para uma**.

Duas propriedades tornam esse bug perigoso: ele só aparece **sob concorrência** (quando o
limite mais importa), e **escalar piora** — mais workers, mais estouro.

**Por que Lua resolve.** O Redis executa cada script atomicamente: enquanto ele roda,
nenhum outro comando é processado. A janela deixa de existir. Bônus: uma ida à rede em vez
de duas.

**Dois detalhes de implementação que a banca pode notar:**

```lua
return { allowed, math.floor(tokens * 1000), retry_after_ms }
```

As fichas voltam **multiplicadas por 1000** porque o protocolo do Redis trunca números de
retorno para inteiro — devolver `0.85` chegaria como `0`.

```lua
local now_ms = tonumber(ARGV[3])   -- vem do CLIENTE, não de redis.call('TIME')
```

Duas razões: **testabilidade** (o teste de paridade injeta o mesmo timestamp nas duas
implementações e compara) e **histórico de replicação** (scripts que leem o relógio eram
considerados não determinísticos em versões antigas do Redis).

### `src/apt/resilience/rate_limiter.py` — a fachada

**Dois eixos, e a ordem entre eles importa:**

```python
# 1) eixo do CONTEÚDO primeiro (mais restritivo)
content_ok, _, content_retry = await self._consume(content_bucket_key(url), ...)
if not content_ok:
    return RateLimitDecision(allowed=False, limited_by="content", ...)

# 2) eixo da PLATAFORMA depois
platform_ok, tokens, platform_retry = await self._consume(platform_bucket_key(p), ...)
```

Se o conteúdo nega, **nunca tocamos** o balde da plataforma — e portanto não gastamos uma
ficha da cota global numa requisição que não vai sair. Na ordem inversa, vazaríamos fichas:
consumiríamos da plataforma, o conteúdo negaria, o envio não aconteceria, e a ficha
ficaria perdida. Sob carga, esse vazamento reduziria a vazão efetiva bem abaixo do
configurado — um bug de "está mais lento do que deveria" difícil de rastrear.

**A decisão de degradação (fail-open):**

```python
except Exception as exc:
    logger.error("rate_limiter.unavailable_fail_open", ...)
    return RateLimitDecision(allowed=True)
```

Se o Redis cai, **permitimos** o envio. Fail-closed pararia o sistema inteiro. E as outras
camadas continuam ativas: o bulkhead ainda limita concorrência, o retry ainda espaça
tentativas, e o 429 da plataforma ainda chega. Perdemos a precisão do limite, não todo o
controle. Ver TRADE-OFFS item 2.

**Por que `reset()` usa `SCAN` e não `KEYS`:** `KEYS` percorre todo o keyspace num único
comando **bloqueante**. Num Redis com muitas chaves, isso congela o servidor por centenas
de milissegundos — e como o rate limiter está no caminho crítico, congelaria o sistema
junto.

### `src/apt/resilience/breaker_state.py` + `lua/circuit_breaker.lua`

**A máquina de estados:**

```
CLOSED ── N falhas consecutivas ──▶ OPEN ── cooldown ──▶ HALF_OPEN ── M sucessos ──▶ CLOSED
                                     ▲                       │
                                     └──── qualquer falha ────┘
```

**Três decisões de projeto embutidas, e cada uma tem um teste:**

**1. Sucesso zera o contador de falhas.** O gatilho é "N falhas **em sequência**", não "N
falhas no total". Falhas isoladas e espaçadas fazem parte da vida de qualquer chamada de
rede — abrir o circuito por causa delas tornaria o sistema desnecessariamente frágil.

**2. Sucesso atrasado com o circuito OPEN não fecha o circuito.** Acontece quando um envio
já estava em voo no instante em que o circuito abriu. A resposta é **mais antiga** que a
decisão de abrir; tratá-la como evidência de recuperação fecharia o circuito com base em
informação obsoleta.

**3. Falha atrasada com o circuito OPEN não reinicia o cooldown.** Se reiniciasse, um
sistema com muitas requisições em voo no momento da abertura poderia manter o circuito
aberto **indefinidamente**, mesmo depois da plataforma ter voltado — nunca sondaria a
recuperação.

**Por que `half_open_probes` é limitado.** Liberar todo o tráfego acumulado de uma vez
seria uma rajada exatamente sobre um serviço que acabou de se recuperar — e o derrubaria
outra vez.

### `src/apt/resilience/bulkhead.py`

**A analogia.** Compartimentos estanques de navio: se um inunda, os outros seguem secos.

**O teste que guarda o modo de falha mais perigoso:**

```python
async def test_timeout_nao_vaza_slot(self):
    # 5 timeouts consecutivos, e a capacidade original continua disponível
```

Quando `asyncio.wait_for` estoura, ele **cancela** a corrotina do `Semaphore.acquire`. O
`asyncio.Semaphore` trata o cancelamento corretamente e não deixa um "acquire fantasma".
Uma implementação caseira perderia um slot por timeout, e o compartimento se estreitaria
com o tempo — a plataforma pararia de ser atendida depois de horas de carga.

**Fail-fast, não espera.** Sem slot em 2s, o envio é recusado. Espera sem limite
transformaria o semáforo numa **fila invisível**: as tarefas não apareceriam em lugar
nenhum (nem no broker, nem em execução), a memória cresceria em silêncio e a latência
medida perderia significado.

### `src/apt/resilience/retry.py`

**Full jitter:**

```python
ceiling = min(cap, base * (2 ** min(attempt - 1, 30)))
return max(1, int(random.uniform(0, ceiling)))
```

Sorteio no intervalo **inteiro** (de 0 ao teto), não ruído pequeno em torno do teto. É a
variante recomendada pela AWS no artigo *Exponential Backoff and Jitter*.

Sem jitter, todos os clientes que falharam no mesmo instante voltam no **mesmo instante** —
o backoff espaça as tentativas mas **mantém a sincronia**, que é justamente o problema.

*O custo:* pode sortear um atraso muito curto. Para uma tarefa isolada é ineficiente; para
o conjunto, é o que importa — a média cai pela metade, mas a **variância** (que evita a
rajada sincronizada) é máxima.

**`min(attempt - 1, 30)` no expoente:** limita o expoente para o caso de `attempt` vir
corrompido de um payload. Sem isso, `2 ** 500` geraria um inteiro absurdo.

### `src/apt/resilience/feature_flags.py`

**O mecanismo de cache + invalidação:**

```
cache local (TTL 2s)  +  evento fanout `flags_changed`
```

Ler o Redis a cada mensagem somaria uma ida à rede no caminho crítico. O cache elimina
quase todas; o evento de fanout torna a mudança **imediata** quando ela de fato acontece.
O TTL é apenas a rede de segurança para o caso do evento se perder.

**A decisão defensiva:**

```python
except Exception:
    logger.warning("feature_flags.refresh_failed", note="mantendo os valores em cache")
```

Falha de Redis **mantém** o cache atual em vez de voltar aos padrões. Se um operador
desligou o rate limiter deliberadamente e o Redis pisca, reverter ao padrão **religaria** o
limiter no meio de uma operação — comportamento surpresa causado por falha de
infraestrutura sem relação com a decisão.

Todas as proteções começam **ligadas**: flag ausente nunca significa "desligue a
proteção".

> ### Perguntas prováveis sobre resiliência
>
> **"Por que o algoritmo existe duas vezes, em Python e em Lua?"**
> A versão Lua é obrigatória (atomicidade). A Python entrega testabilidade (17 casos de
> borda em milissegundos, sem Docker) e legibilidade (é a explicação do algoritmo). O
> risco de divergência é coberto por um **teste de paridade** que roda a mesma sequência
> nas duas e compara os resultados.
>
> **"Por que não bastou `INCR`?"**
> `INCR` é atômico, mas não sabe fazer refill baseado em tempo, nem limitar o saldo à
> capacidade, nem calcular quanto falta para a próxima ficha. Daria um contador por
> janela fixa — que sofre do efeito de borda: 20 requisições em `12:00:00.999` e 20 em
> `12:00:01.001` são 40 em 2ms, dentro do limite formal de cada janela.
>
> **"O que acontece se dois workers pedirem o último token ao mesmo tempo?"**
> Um recebe `allowed=1`, o outro `allowed=0` com `retry_after_ms`. O script Lua é atômico:
> o Redis não processa nenhum outro comando enquanto ele roda. Isso está verificado em
> `test_concorrencia_nao_estoura_o_limite` — 50 corrotinas simultâneas, exatamente
> `capacity` passam.
>
> **"E se o Redis cair?"**
> Fail-open: permitimos o envio, logamos em ERROR. Fail-closed transformaria uma queda de
> Redis em indisponibilidade total. As demais camadas continuam ativas. É a decisão mais
> discutível do projeto, e está registrada em TRADE-OFFS item 2.
>
> **"Por que WATCH/MULTI não serviria?"**
> Serviria funcionalmente, mas sob **alta contenção** — todos os workers na mesma chave — a
> taxa de retry do `EXEC` dispararia. O Lua resolve em uma ida à rede, sempre.

---

## 5. Mensageria

### `src/apt/messaging/topology.py`

**O que faz.** Declara exchanges, filas, DLX/DLQ e as três filas de retry. Idempotente —
API e worker chamam a mesma função no boot.

**O detalhe mais frágil do sistema inteiro:**

```python
arguments={
    "x-message-ttl": ttl_ms,
    "x-dead-letter-exchange": EXCHANGE_TASKS,
    # Sem `x-dead-letter-routing-key`: assim o RabbitMQ preserva a routing key
    # original da mensagem, que é a plataforma.
}
```

Se alguém definir `x-dead-letter-routing-key` aqui, **todo retry vai para a fila errada** —
e o bug é **silencioso**: as mensagens circulariam sem nunca chegar ao consumidor certo.
Há um teste de integração que verifica exatamente esse caminho.

**Por que filas de TTL e não `sleep()` no worker.** Com `prefetch=1`, um worker que dorme
30s segura o seu único slot e **para de consumir**. Cinco workers em backoff longo travam o
sistema enquanto a fila cresce. Aqui o tempo passa **dentro do broker**.

**Por que três filas fixas e não TTL por mensagem.** No RabbitMQ, a expiração só é
avaliada quando a mensagem chega à **cabeça** da fila. Uma mensagem com TTL de 30s
publicada antes de outra com TTL de 1s bloqueia a segunda pelos 30 segundos inteiros
(*head-of-line blocking*).

**Teto de segurança nas filas:**

```python
"x-max-length": 100_000,
"x-overflow": "drop-head",
```

Se o consumo parar por muito tempo, o broker descarta as mensagens **mais antigas** em vez
de estourar a memória e derrubar o cluster. Perder tarefa antiga é ruim; perder o broker é
pior.

### `src/apt/messaging/publisher.py`

**Três garantias:**

1. `DeliveryMode.PERSISTENT` — o broker grava em disco.
2. `publisher_confirms=True` — `publish()` só retorna após confirmação do broker. Sem
   isso, retorna quando a mensagem entra no buffer TCP local, e uma queda do broker nesse
   instante perde a tarefa em silêncio.
3. `connect_robust` — reconecta e redeclara a topologia sozinho.

### `src/apt/messaging/consumer.py`

**`prefetch=1` — a decisão que cabe numa linha:**

```python
await channel.set_qos(prefetch_count=settings.worker_prefetch)  # = 1
```

O padrão do AMQP é ilimitado. Com prefetch alto, o primeiro worker a conectar puxa
**todas** as mensagens disponíveis para o buffer local e as processa em série — enquanto as
outras réplicas ficam paradas, com a fila vazia. A fila parece equilibrada no painel, mas
não está.

**A rede de segurança no wrapper do handler:**

```python
except Exception as exc:
    logger.exception("consumer.handler_failed", ...)
    if not raw.processed:
        await raw.nack(requeue=False)
```

Se o handler estourar sem tratar, a mensagem **não pode ficar sem resposta**: sem ack nem
nack, ela ficaria "unacked" no broker até a conexão cair, **travando o slot de prefetch**
daquele worker.

**Shutdown com drenagem:**

```python
while self._in_flight > 0 and loop.time() < deadline:
    await asyncio.sleep(0.2)
```

Fechar a conexão no meio de um envio faria o broker reentregar a mensagem — e a plataforma
receberia o envio **duas vezes**.

> ### Perguntas prováveis
>
> **"Por que fanout para o controle e topic para as tarefas?"**
> Uma tarefa deve ser processada por **um** worker — topic com fila compartilhada faz
> exatamente isso. Uma invalidação de flag deve chegar a **todos** — com topic, apenas um
> receberia e os outros ficariam com cache velho.
>
> **"Por que ack manual?"**
> Com ack automático, o broker considera a mensagem entregue no instante em que a manda
> pela rede. Um `kill -9` no worker perderia a tarefa em voo. Com ack manual, ela volta
> para a fila — a garantia at-least-once em que o sistema se baseia.
>
> **"At-least-once significa que pode haver envio duplicado?"**
> Sim, e é uma limitação conhecida. Se o worker morre entre enviar e dar ack, a tarefa é
> reprocessada. A solução seria idempotência ponta a ponta, fora do escopo (TRADE-OFFS
> item 3). Escolhemos at-least-once porque duplicar é recuperável e perder não é.

---

## 6. Scheduler

### `src/apt/scheduling/jitter.py`

**O problema que resolve.** Respeitar o limite de vazão **não basta**. Um sistema que envia
exatamente 16 requisições no primeiro milissegundo de cada segundo respeita 16 req/s e
exibe um padrão obviamente automatizado: intervalos idênticos, variância zero. Os
mecanismos de detecção olham a **forma** da distribuição, não só o volume.

**As três estratégias:**

| Estratégia | Modelo | Quando |
|---|---|---|
| `UNIFORM` | sorteio uniforme na janela | simples, variância limitada |
| `EXPONENTIAL` | processo de Poisson | modelo estatístico de chegadas independentes |
| `HUMANIZED` | exponencial × perfil diário | **padrão** — volume acompanha ritmo humano |

**O truque da taxa fracionária:**

```python
whole = int(base_count)
fraction = base_count - whole
count = whole + (1 if r.random() < fraction else 0)
```

Com 30 envios/min e tick de 1s, o alvo é **0.5 tarefa por tick**. Truncar levaria a zero
para sempre e a campanha nunca sairia; arredondar para cima entregaria o dobro. Tratamos a
fração como **probabilidade** — ao longo de muitos ticks, a média converge. Verificado em
`test_taxa_fracionaria_converge_na_media` (2000 ticks, tolerância 12%).

**O piso do perfil diário:**

```python
_MIN_ACTIVITY = 0.15
```

Sem ele, a hora de atividade 0.12 faria o intervalo crescer 8× e a campanha praticamente
parar de madrugada — atrasando o orçamento de forma que não daria para compensar.

### `src/apt/scheduling/dispatcher.py`

**A decisão de consistência mais importante do arquivo:**

```python
# 1) grava a intenção
task_id = await TaskRepository.create(conn, ...)
# 2) publica
await self._publisher.publish_task(message)
```

Gravamos **antes** de publicar. As duas falhas possíveis são assimétricas:

| Falha | Consequência |
|---|---|
| banco OK, publish falha | linha `pending` que nunca será consumida — **tarefa órfã visível e auditável** |
| publish OK, banco falha | o worker recebe `task_id` inexistente — **envio sem registro** |

Registro sem envio é melhor que envio sem registro. A solução definitiva seria
Transactional Outbox, fora do escopo.

**A rede de segurança do loop:**

```python
while not self._stopping.is_set():
    try:
        await self.tick()
    except Exception as exc:
        logger.exception("dispatcher.tick_failed", error=str(exc))
```

Se uma exceção escapasse deste loop, a background task morreria **em silêncio** e o
sistema deixaria de gerar tarefas — sem erro visível, apenas campanhas ativas que nunca
enviam nada. Foi o comportamento observado na primeira versão.

**`wait_for` em vez de `sleep`:**

```python
with suppress(TimeoutError):
    await asyncio.wait_for(self._stopping.wait(), timeout=tick_seconds)
```

Esperar no **evento** de parada com timeout faz o shutdown ser imediato; com `sleep`, o
processo teria de aguardar o tick corrente terminar de dormir.

---

## 7. API

### `src/apt/api/schemas.py`

Três funções simultâneas: **validar** a entrada antes de qualquer código nosso rodar,
**documentar** (o OpenAPI sai daqui) e **definir** a forma da resposta, evitando devolver
acidentalmente uma coluna interna.

**Validação com justificativa:**

```python
contents: Annotated[list[ContentIn], Field(min_length=1, max_length=500)]
```

Pelo menos uma URL é **obrigatória**. Uma campanha sem pool não tem o que enviar, e o
dispatcher apenas emitiria WARNING a cada tick. Barrar na entrada é melhor que aceitar uma
campanha inerte.

```python
@field_validator("contents")
def _reject_duplicate_urls(cls, value): ...
```

O banco tem `UNIQUE (campaign_id, content_url)` e o `ON CONFLICT` do repositório faria a
segunda ocorrência **sobrescrever silenciosamente** o peso da primeira. Quem mandou a
mesma URL com pesos diferentes provavelmente errou — melhor dizer isso do que escolher um
dos pesos por ele.

### `src/apt/api/routers/campaigns.py`

**A ordem dentro da transação:**

```python
campaign_id = await CampaignRepository.create(...)      # nasce DRAFT
await ContentRepository.add_many(...)                   # cadastra o pool
if payload.activate:
    await CampaignRepository.set_status(..., ACTIVE)     # só AGORA ativa
```

A campanha nasce `draft` e só vira `active` **depois** do pool cadastrado. Como o
dispatcher só enxerga `active`, isso elimina a janela em que ele encontraria uma campanha
ativa sem nenhuma URL.

**Uma honestidade no docstring de `pause_campaign`:** pausar **não** cancela as tarefas já
publicadas. Elas continuam sendo processadas. Cancelar mensagens em voo exigiria purgar a
fila, o que descartaria também tarefas de outras campanhas da mesma plataforma.

### `src/apt/api/routers/health.py`

**A distinção que evita o loop de restart:**

| Endpoint | Checa dependências? | Falha significa |
|---|---|---|
| `/health/live` | **não** | **reinicie** o container |
| `/health/ready` | sim (Postgres, Redis, dispatcher) | **pare de mandar tráfego** — não reinicie |

Se `live` checasse o banco, uma indisponibilidade de 30s do Postgres faria o orquestrador
reiniciar todos os containers da aplicação — que voltariam e falhariam de novo, porque o
problema nunca esteve neles. É o clássico loop de restart causado por health check mal
desenhado.

### `src/apt/api/main.py`

**O startup não aborta:**

```python
if not await db_health():
    state.startup_errors.append("postgres indisponivel no startup")
    logger.error("api.startup_postgres_unavailable")
```

Registra o erro e deixa `/health/ready` responder 503. O container sobe, fica observável e
volta ao normal sozinho quando a dependência se recupera — em vez de entrar em
CrashLoopBackOff, onde não dá nem para ler o log com calma.

**O handler de exceção devolve o `correlation_id`:**

```python
content={"detail": "erro interno", "correlation_id": cid, "hint": "..."}
```

Quem recebeu o 500 pode informar o id, e o log completo — com stack trace — é encontrado
por ele. Sem isso, "deu erro 500" é uma queixa sem rastro.

---

## 8. Worker

### `src/apt/worker/main.py` — as cinco camadas

**A ordem, e por que ela é essa:**

| # | Camada | Custo |
|---|---|---|
| 1 | Feature flags | cache local, ~zero |
| 2 | Bulkhead | semáforo em memória |
| 3 | Circuit breaker | 1 ida ao Redis |
| 4 | Rate limiter | 2 idas ao Redis |
| 5 | Envio | chamada de rede |

Cada camada é **mais barata que a seguinte**, e recusar cedo evita gastar o recurso da
próxima.

**O detalhe que mais importa:** o breaker vem **antes** do rate limiter. Consumir uma ficha
do balde para depois descobrir que o circuito está aberto desperdiçaria cota — e **a ficha
não volta**.

**O `finally` obrigatório:**

```python
finally:
    bulkhead.release()
```

Um `return` antecipado ou uma exceção sem o release vazaria um slot **permanentemente**, e
após N vazamentos a plataforma pararia de ser atendida por aquele worker.

**Tratamento do `Retry-After`:**

```python
if result.retry_after_ms:
    tier = tier_for_retry_after(result.retry_after_ms)   # sem jitter
else:
    tier, delay = tier_for_attempt(attempt)              # com jitter
```

Quando a plataforma informa o prazo, é **instrução**, não estimativa. Sortear um número
menor seria desobedecer — e ignorar um `Retry-After` explícito é a forma mais rápida de
escalar de throttling para bloqueio.

**O teto de adiamentos:**

```python
MAX_DEFERS = 200
```

Adiamento não consome tentativa, então sem teto uma campanha configurada muito acima da
capacidade produziria tarefas circulando indefinidamente.

**SIGTERM:**

```python
loop.add_signal_handler(sig, worker.request_stop)
```

`docker compose down` manda SIGTERM. Sem tratar, o processo morre imediatamente e as
tarefas em voo são abandonadas — o broker as reentrega, mas o envio pode já ter saído,
gerando duplicidade.

### `src/apt/worker/sender.py`

**Um pool de conexões por plataforma** — a terceira camada do bulkhead:

```
fila dedicada → isola no BROKER
semáforo      → isola os SLOTS DE EXECUÇÃO
pool HTTP     → isola as CONEXÕES DE REDE
```

Sem a terceira, um `AsyncClient` compartilhado teria pool comum: requisições lentas do
Instagram ocupariam conexões e as do YouTube esperariam — reintroduzindo o acoplamento que
as duas primeiras eliminaram.

**`follow_redirects=False`** de propósito: um 3xx inesperado deve aparecer como anomalia,
não ser seguido silenciosamente para um destino que não foi o que pedimos.

**`send()` nunca propaga exceção** — qualquer falha de rede vira um `Outcome`. O worker
precisa sempre poder decidir entre ack, retry e DLQ.

---

## 9. Simulador de plataformas

### `src/apt/platform_sim/throttle.py`

**A decisão que torna o teste honesto:** o simulador usa **janela deslizante**, o nosso
limiter usa **token bucket**.

Se as duas pontas usassem o mesmo algoritmo com os mesmos parâmetros, o nosso limiter
acertaria o limite **por construção** — o teste provaria que `16 < 20`, aritmética, não
engenharia. Com algoritmos diferentes, as janelas não se alinham, e é por isso que a
margem de 20% existe.

**`peak_rps` — o número mais importante do relatório:**

```python
self.peak_rps = max(self.peak_rps, len(self.window))
```

É o pico que a **plataforma** observou. Se ficou abaixo do limite dela durante todo o
teste, o rate limiter cumpriu o objetivo — medido por quem imporia a punição, não por nós.

**Auto-expiração da falha:**

```python
return time.monotonic() < self.expires_at
```

Permite observar o circuito abrir **e** fechar numa única execução, sem intervenção manual
no meio da medição — o momento exato de uma chamada manual influenciaria o resultado.

---

## 10. Observabilidade

### `src/apt/observability/metrics.py`

**A nota sobre cardinalidade** é o ponto que a banca pode explorar:

Nenhuma métrica usa `content_url`, `task_id` ou `campaign_id` como label. Cada valor
distinto criaria uma **série temporal permanente** no Prometheus, e uma campanha com mil
URLs geraria mil séries que continuariam consumindo memória depois da campanha terminar.
Alta cardinalidade é papel do Postgres.

**Buckets do histograma ajustados ao cenário:**

```python
buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
```

O simulador responde em 5–40ms. Os buckets padrão do `prometheus_client` deixariam quase
toda a amostra no primeiro bucket e o p95 seria inutilizável.

**`set_circuit_state` devolve -1 para estado desconhecido**, em vez de estourar: uma
métrica nunca deve derrubar o caminho de execução que a produz. O valor negativo é
visivelmente anômalo num gráfico.

---

## 11. Q&A geral

Perguntas que atravessam módulos.

**"Qual é a tese central do projeto, em uma frase?"**
Que o estado do rate limiter tem de ser compartilhado, porque um limiter em memória de
processo multiplica a vazão pelo número de réplicas — e portanto falha exatamente ao
escalar.

**"Como vocês provam isso?"**
`tests/load/scale_test.py`: roda a mesma carga com 1, 3 e 5 workers e verifica que o pico
observado pela plataforma **não cresce**. Com limiter local, cresceria ~5×.

**"Por que 6 padrões e não os 7 recomendados?"**
Traffic Sharding foi removido com justificativa documentada (ADR-011), como permite a
Seção 8 do documento da disciplina. O benefício se sobrepunha ao que o eixo por conteúdo
do rate limiter já entrega, e o custo de defesa oral era o mais alto da lista. O mínimo
exigido é 3.

**"Qual é a maior limitação conhecida do sistema?"**
Falta idempotência ponta a ponta. A semântica at-least-once do RabbitMQ permite que um
worker que morre entre enviar e dar ack cause um segundo envio. Está registrado em
TRADE-OFFS item 3.

**"Por que os testes unitários não precisam de Docker?"**
Porque a lógica crítica foi escrita como **função pura**: token bucket, máquina de estados
do breaker, jitter e backoff não fazem I/O e recebem o tempo como parâmetro. Isso permite
testar exaustivamente os casos de borda — relógio para trás, refill fracionário,
transição após cooldown — em milissegundos.

**"Onde vocês erraram e corrigiram?"**
Três correções que valem contar:

1. **Contador único para falhas e adiamentos.** Uma tarefa adiada 4× pelo rate limiter ia
   para a DLQ sem nunca ter sido enviada. Corrigido separando `attempt` de `defers`.
2. **Dispatcher morrendo em silêncio.** Uma exceção não tratada no loop matava a
   background task e o sistema parava de gerar tarefas sem nenhum erro visível. Corrigido
   com tratamento por tick.
3. **`docker compose` sem `depends_on: service_healthy`.** A API subia antes do Postgres
   aceitar conexão e morria no boot.

**"Por que o worker consulta o breaker antes do rate limiter?"**
Porque consumir uma ficha do balde para depois descobrir que o circuito está aberto
desperdiçaria cota, e a ficha não volta.

**"Se eu desligar o Redis, o que acontece?"**
O rate limiter e o breaker passam a permitir tudo (fail-open), logando em ERROR. O bulkhead
local, o retry com backoff e o tratamento de 429 continuam funcionando. A vazão pode
exceder o limite durante a queda — decisão deliberada, discutida em TRADE-OFFS item 2.

**"Como vocês garantem que o script Lua e a versão Python não divergem?"**
`test_paridade_com_a_implementacao_de_referencia`: 30 passos da mesma sequência, com o
mesmo timestamp injetado, comparando `allowed`, `tokens_remaining` e `retry_after_ms`.
