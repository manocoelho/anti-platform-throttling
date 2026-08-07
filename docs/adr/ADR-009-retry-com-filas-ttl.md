# ADR-009 — Retry com filas de TTL, não `sleep` no worker

**Status:** Aceita
**Origem:** Projeto 03

## Contexto

Uma tarefa pode falhar de duas formas distintas, e a distinção define tudo:

| | O que aconteceu | Contador |
|---|---|---|
| **Falha de envio** | A requisição saiu e a plataforma recusou (429, 5xx, timeout) | `attempt` |
| **Adiamento** | Nós decidimos não enviar (rate limiter negou, circuito aberto, bulkhead cheio) | `defers` |

Nos dois casos a tarefa precisa voltar para a fila depois de um intervalo. A
pergunta é **onde esse tempo passa**.

## Decisão

### 1. O tempo passa dentro do broker, em filas de TTL

Três filas sem consumidor, cada uma com `x-message-ttl` fixo e
`x-dead-letter-exchange` apontando **de volta** para `apt.tasks`:

```
apt.retry.1  ->  TTL  1s  --expira-->  apt.tasks  ->  apt.tasks.<plataforma>
apt.retry.2  ->  TTL  5s  --expira-->  apt.tasks  ->  apt.tasks.<plataforma>
apt.retry.3  ->  TTL 30s  --expira-->  apt.tasks  ->  apt.tasks.<plataforma>
```

O worker publica na fila de espera e dá ack na mensagem original. Fica livre
imediatamente.

### 2. Dois contadores, não um

`attempt` e `defers` viajam separados no payload (`SendTaskMessage`).

**Por que isso importa.** Num sistema saudável sob carga, os adiamentos são
**frequentes e esperados** — é exatamente o rate limiter fazendo o seu trabalho. Se
eles incrementassem `attempt`, uma tarefa adiada `max_attempts` vezes iria para a
DLQ **sem nunca ter sido enviada**. O sistema descartaria trabalho legítimo
justamente quando estivesse se protegendo corretamente.

Foi o primeiro bug que apareceu ao juntar rate limiter e retry, e a separação dos
contadores é a correção. `defers` tem um teto próprio e generoso (200) apenas para
evitar circulação infinita.

### 3. Backoff exponencial com **full jitter**

```python
delay = random.uniform(0, min(cap, base * 2 ** attempt))
```

### 4. `Retry-After` da plataforma tem precedência

Quando a plataforma responde 429 com `Retry-After`, respeitamos o valor dela em vez
do nosso cálculo — e **sem jitter**. É uma instrução, não uma estimativa.

## Alternativas consideradas

### `await asyncio.sleep(delay)` dentro do worker

A forma mais óbvia. **Recusada**, e o motivo é a interação com `prefetch=1`
(ADR-001): o worker que dorme 30 segundos segura o seu único slot e **para de
consumir**. Cinco workers com cinco tarefas em backoff longo travam o sistema
inteiro enquanto a fila cresce.

O adiamento por rate limiter tornaria isso catastrófico: sob demanda acima do
limite, quase toda tarefa é adiada, e os workers passariam a maior parte do tempo
dormindo.

### TTL **por mensagem** em uma fila única

O AMQP permite `expiration` por mensagem, o que daria backoff exponencial contínuo
com jitter exato — tecnicamente melhor que três degraus fixos.

**Recusado por um comportamento documentado do RabbitMQ:** a expiração só é
avaliada quando a mensagem chega à **cabeça** da fila. Uma mensagem com TTL de 30s
publicada antes de outra com TTL de 1s **bloqueia a segunda pelos 30 segundos
inteiros** (*head-of-line blocking*).

Com filas de TTL fixo, toda mensagem numa fila tem o mesmo prazo, e a ordem FIFO
coincide com a ordem de expiração. O jitter continua existindo — é aplicado na
**escolha** da fila.

### Plugin `rabbitmq_delayed_message_exchange`

Resolve o problema de forma elegante: atraso arbitrário por mensagem, sem
head-of-line blocking. Recusado por ser plugin de terceiros que exige habilitação
na imagem do broker — mais uma peça de infraestrutura para justificar, quando os
três degraus já cobrem de "engasgo momentâneo" a "plataforma fora do ar".

### Backoff exponencial **sem** jitter

Recusado pelo *thundering herd*: sem jitter, todos os clientes que falharam no mesmo
instante voltam a tentar no **mesmo instante**. A plataforma que acabou de recusar
200 requisições recebe as mesmas 200 exatamente 1 segundo depois, todas juntas. O
backoff espaça as tentativas no tempo mas **mantém a sincronia** entre elas — que é
justamente o que causa o problema.

### Jitter parcial (ruído pequeno em torno do teto)

Ex.: `delay = teto * random.uniform(0.8, 1.2)`. Recusado porque os clientes ainda se
agrupam perto do teto. O **full jitter** — sorteio no intervalo inteiro, de 0 ao
teto — é a variante recomendada pela AWS no artigo *Exponential Backoff and Jitter*,
e a comparação apresentada lá mostra por quê.

O custo do full jitter é poder sortear um atraso muito curto. Para uma tarefa
isolada isso é ineficiente; para o conjunto, é o que importa: a média do intervalo
cai pela metade, mas a **variância** — que é o que evita a rajada sincronizada — é
máxima.

## Consequências positivas

- **Workers nunca ficam parados esperando.** O tempo passa no broker.
- **Adiamento não gasta tentativa.** Trabalho legítimo não vai para a DLQ por causa
  de autolimitação.
- **Jitter quebra a sincronia** entre clientes que falharam juntos.
- **`Retry-After` respeitado.** Ignorar um pedido explícito da plataforma é a forma
  mais rápida de escalar de throttling para bloqueio.
- **4xx não retentáveis vão direto para a DLQ.** Um 404 não muda de resultado ao ser
  repetido; retentar só gasta cota do rate limiter e atrasa tarefas que teriam
  sucesso.
- **DLQ + tabela `failures`.** A DLQ guarda a mensagem para reprocessamento; a
  tabela responde "quantas tarefas falharam hoje, por plataforma?" com SQL.

## Consequências negativas

- **Backoff em degraus, não contínuo.** O atraso real é arredondado **para cima**
  até o próximo degrau. Esperar um pouco mais é inofensivo; esperar menos
  significaria voltar antes da plataforma ter se recuperado.
- **Três filas a mais na topologia.** A topologia é a parte mais difícil de explicar
  do sistema, e isso contribui. Mitigado com o diagrama no docstring de
  `topology.py`.
- **Publicar antes do ack permite duplicidade.** Se o processo morre entre publicar
  o retry e dar ack, o broker reentrega a original e a tarefa é processada duas
  vezes. A ordem inversa (ack primeiro) perderia a tarefa se a publicação falhasse.
  Escolhemos at-least-once: duplicar é recuperável, perder não é.
- **Detalhe frágil que exige atenção:** a fila de retry **não** define
  `x-dead-letter-routing-key`. Isso é o que faz o RabbitMQ preservar a routing key
  original (a plataforma) quando a mensagem volta. Se alguém definir essa chave, todo
  retry cairá na fila errada — e o bug será **silencioso**: as mensagens circulariam
  sem nunca chegar ao consumidor certo. Está comentado no código e coberto por teste.

## Como validamos

- **`tests/integration/test_messaging.py::TestRetryComTTL::test_retry_volta_para_a_fila_original`**
  — verifica o caminho completo pelo broker, incluindo a preservação da routing key.
  É o teste que protege o detalhe frágil acima.
- **`tests/unit/test_domain.py::TestSendTaskMessage::test_defers_nao_incrementa_attempt`**
  — 50 adiamentos e `attempt` continua zero. Guarda a correção do bug conceitual.
- **`tests/unit/test_retry.py::TestBackoff::test_full_jitter_produz_dispersao_alta`**
  — verifica que o desvio-padrão é fração substancial da média. Num backoff sem
  jitter, o desvio seria **zero** — todos voltariam no mesmo instante.
- `tests/unit/test_retry.py::TestChooseTier` — arredondamento para cima em todos os
  limites de degrau.
- `TestIsRetryableStatus` — 429/408/5xx retentáveis; 400/401/403/404 não.
