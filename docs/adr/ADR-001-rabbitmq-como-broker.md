# ADR-001 — RabbitMQ como message broker

**Status:** Aceita
**Origem:** Projeto 02 — **revisado no Projeto 03**

> **Nota sobre a revisão.** A decisão original foi tomada no Projeto 02, antes de
> existir código. As seções de **Consequências** e **Como validamos** foram
> reescritas aqui com o que a implementação revelou. Duas coisas que o Projeto 02
> não previa: a necessidade de **filas dedicadas por plataforma** (que virou a
> base do Bulkhead, ADR-007) e o mecanismo de **retry via TTL + dead-letter**
> (ADR-009), que só é possível por ser um recurso nativo do RabbitMQ.

## Contexto

O sistema recebe pedidos de campanha via HTTP e precisa enviar milhares de
requisições a plataformas externas, respeitando limites de vazão. Duas
características do problema definem a arquitetura:

1. **O ritmo de entrada é diferente do ritmo de saída.** O administrador cria uma
   campanha de 10.000 envios num único POST. As plataformas aceitam 20 req/s.
   Processar de forma síncrona significaria uma requisição HTTP de 8 minutos.
2. **O trabalho precisa sobreviver a falhas.** Se o processo que envia morre no
   meio, as tarefas pendentes não podem desaparecer.

Precisamos de um intermediário durável entre receber e processar.

## Decisão

Adotamos **RabbitMQ** como message broker, com a topologia:

- exchange **topic** `apt.tasks` → uma fila por plataforma
- exchange **fanout** `apt.control` → uma fila privada por worker
- **DLX + DLQ** para falhas terminais
- três **filas de retry com TTL** para o backoff

## Alternativas consideradas

**Tabela no Postgres como fila (`SELECT ... FOR UPDATE SKIP LOCKED`).**
Tecnicamente viável e teria eliminado um serviço da arquitetura — usamos
exatamente esse padrão no dispatcher para reservar campanhas. Recusamos porque
faltariam três coisas que usamos de fato: entrega push (com polling, o worker
consulta o banco em loop mesmo sem trabalho), dead-lettering nativo, e o
mecanismo de TTL que resolve o backoff sem bloquear worker (ADR-009).
Implementar isso sobre SQL seria reescrever um broker.

**Apache Kafka.** É a ferramenta certa para *streaming* de eventos com replay e
retenção longa. O nosso caso é *messaging*: cada tarefa é consumida uma vez por um
worker qualquer e depois não interessa mais. Kafka particiona por chave e ordena
dentro da partição — garantias que não precisamos — e o custo é operacional
(ZooKeeper/KRaft, gestão de offsets, rebalanceamento de consumer group).
Complexidade sem contrapartida aqui.

**Redis como fila (`LPUSH`/`BRPOP`).** Já temos Redis no stack (ADR-003), então
seria "de graça". Recusamos por causa das garantias de entrega: `BRPOP` remove a
mensagem no momento da leitura. Se o worker morre depois de ler e antes de
processar, a tarefa desaparece. Existem padrões para contornar (`RPOPLPUSH` com
lista de processamento, ou Redis Streams com consumer groups), mas nenhum entrega
ack manual, dead-lettering e roteamento com a maturidade do RabbitMQ.

## Consequências positivas

- **Desacoplamento real.** A API responde em milissegundos; o processamento
  acontece depois. Reiniciar workers não afeta a API.
- **Escalabilidade horizontal com competing consumers.** Subir uma réplica de
  worker é `--scale worker=5`. Nenhuma mudança de código, nenhuma coordenação.
- **`prefetch=1` dá balanceamento justo.** O broker entrega uma mensagem por
  worker livre — a base do padrão Load Balancing no projeto.
- **Ack manual dá garantia at-least-once.** `kill -9` num worker faz o broker
  reentregar a tarefa em voo a outro.
- **Filas dedicadas por plataforma viabilizam o Bulkhead** (ADR-007) — um recurso
  que não custa nada além de declarar mais uma fila.
- **DLX/TTL nativo viabiliza o retry sem bloquear worker** (ADR-009).
- **Painel de gerenciamento.** Ver a profundidade das filas em tempo real durante
  a apresentação vale mais que qualquer slide.

## Consequências negativas

- **Um serviço a mais para operar.** RabbitMQ precisa de monitoramento próprio
  (profundidade de fila, memória, conexões). Fora do escopo da POC, mas real.
- **Novo ponto de falha.** Se o broker cai, nenhuma tarefa nova é processada.
  Mitigamos parcialmente com `connect_robust` (reconexão automática) e mensagens
  persistentes, mas a POC não tem cluster — é um SPOF assumido.
- **Semântica at-least-once significa duplicidade possível.** Se um worker morre
  entre enviar e dar ack, a tarefa é reprocessada e a plataforma recebe o envio
  duas vezes. A solução seria idempotência ponta a ponta, que ficou fora do
  escopo (ver `docs/TRADE-OFFS.md`).
- **A topologia é a parte mais difícil de explicar do sistema.** Quatro
  exchanges, seis filas e o caminho do retry por dead-lettering não são óbvios.
  Foi por isso que `src/apt/messaging/topology.py` tem o diagrama e a
  justificativa no próprio docstring.
- **Mensagem persistente custa latência de publicação.** Cada publish espera o
  broker gravar em disco. Aceitamos: perder tarefa num restart seria pior.

## Como validamos

- `tests/integration/test_messaging.py::TestRetryComTTL::test_retry_volta_para_a_fila_original`
  — verifica o caminho completo do backoff pelo broker, incluindo a preservação
  da routing key original (que é o detalhe onde um erro seria silencioso).
- `tests/integration/test_messaging.py::TestFanoutDeControle` — comprova que o
  fanout entrega a **todas** as filas, e não a uma.
- `tests/load/scale_test.py` — mede a distribuição de envios entre réplicas via
  `GET /admin/workers`, comprovando o balanceamento com `prefetch=1`.
