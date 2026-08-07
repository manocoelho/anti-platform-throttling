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

Seis filas sem consumidor (uma por plataforma x degrau), cada uma com
`x-message-ttl` fixo, `x-dead-letter-exchange` apontando **de volta** para
`apt.tasks` e `x-dead-letter-routing-key` **declarado explicitamente** como a
própria plataforma:

```
apt.retry.youtube.1     ->  TTL  1s  --expira-->  apt.tasks.youtube
apt.retry.youtube.2     ->  TTL  5s  --expira-->  apt.tasks.youtube
apt.retry.youtube.3     ->  TTL 30s  --expira-->  apt.tasks.youtube
apt.retry.instagram.1   ->  TTL  1s  --expira-->  apt.tasks.instagram
apt.retry.instagram.2   ->  TTL  5s  --expira-->  apt.tasks.instagram
apt.retry.instagram.3   ->  TTL 30s  --expira-->  apt.tasks.instagram
```

O worker publica na fila de espera e dá ack na mensagem original. Fica livre
imediatamente.

**Uma fila por plataforma, não só por degrau — e não é só estética.** A primeira
versão desta decisão tinha 3 filas (uma por degrau, compartilhadas entre
plataformas) e omitia `x-dead-letter-routing-key`, na suposição de que o RabbitMQ
preservaria a routing key ORIGINAL da mensagem (a plataforma) ao expirar o TTL. Era
um bug real: a routing key preservada é a que a mensagem tem ao ENTRAR na fila de
retry — `tier.N`, usada só para escolher a fila de espera — não a plataforma. Como
`apt.tasks` não tinha binding para `tier.N`, toda mensagem que expirava numa fila de
retry chegava inroteável e era descartada em silêncio: **toda tarefa adiada era
perdida para sempre**, nunca retornando ao fluxo. Ver "Consequências negativas"
abaixo para a análise completa e a evidência de reprodução.

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
- **Seis filas na topologia, não três.** A topologia é a parte mais difícil de
  explicar do sistema, e a segregação por plataforma (necessária para corrigir o bug
  abaixo) dobra o número de filas de retry. Mitigado com o diagrama no docstring de
  `topology.py`.
- **Publicar antes do ack permite duplicidade.** Se o processo morre entre publicar
  o retry e dar ack, o broker reentrega a original e a tarefa é processada duas
  vezes. A ordem inversa (ack primeiro) perderia a tarefa se a publicação falhasse.
  Escolhemos at-least-once: duplicar é recuperável, perder não é.
- **Bug real, corrigido nesta entrega:** a primeira versão desta decisão tinha 3
  filas de retry (uma por degrau, compartilhadas entre plataformas) e não definia
  `x-dead-letter-routing-key` — supondo que o RabbitMQ preservaria a routing key
  ORIGINAL da mensagem (a plataforma) ao expirar o TTL. Na prática, a routing key
  preservada é a que a mensagem tem ao **entrar** na fila de retry — `tier.N`,
  usada só para escolher a fila de espera — não a plataforma. `apt.tasks` não tinha
  binding para `tier.N`: a mensagem chegava inroteável e era descartada **em
  silêncio**. Toda tarefa adiada (rate limiter, bulkhead ou circuit breaker) era
  perdida ao expirar o TTL, em vez de retornar ao fluxo. Confirmado publicando
  diretamente no exchange `apt.retry` com `aio_pika`, fora do pytest: a mensagem
  não aparecia nem na fila de retry, nem em `apt.tasks.<plataforma>`, nem na DLQ,
  depois do TTL — ela simplesmente desaparecia. Corrigido segregando as filas por
  plataforma×degrau e declarando `x-dead-letter-routing-key` explicitamente (a
  correção que este ADR agora descreve). Detalhe completo em
  [TRADE-OFFS.md, item 14](../TRADE-OFFS.md).

## Como validamos

- **`tests/integration/test_messaging.py::TestRetryComTTL::test_retry_volta_para_a_fila_original`**
  — verifica o caminho completo pelo broker: publica, espera o TTL, confirma que a
  mensagem retorna a `apt.tasks.youtube` com o `attempt` intacto. Foi este teste que
  pegou o bug descrito acima quando a fila ainda era compartilhada por degrau.
- **`tests/unit/test_domain.py::TestSendTaskMessage::test_defers_nao_incrementa_attempt`**
  — 50 adiamentos e `attempt` continua zero. Guarda a correção do bug conceitual.
- **`tests/unit/test_retry.py::TestBackoff::test_full_jitter_produz_dispersao_alta`**
  — verifica que o desvio-padrão é fração substancial da média. Num backoff sem
  jitter, o desvio seria **zero** — todos voltariam no mesmo instante.
- `tests/unit/test_retry.py::TestChooseTier` — arredondamento para cima em todos os
  limites de degrau.
- `TestIsRetryableStatus` — 429/408/5xx retentáveis; 400/401/403/404 não.
