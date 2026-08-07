# ADR-010 — Scheduler como background task da API

**Status:** Aceita
**Origem:** Projeto 03 (o Projeto 02 não previa um scheduler)

## Contexto

O Projeto 02 desenhou três containers de aplicação: API, Worker e (implicitamente)
nada entre eles. Ao implementar ficou claro que falta uma peça: alguém precisa
transformar "mande 10.000 envios a 600/min" em **tarefas individuais publicadas na
fila, espaçadas no tempo**.

Essa é uma responsabilidade distinta das outras duas. A API recebe e responde; o
worker consome e envia. Materializar campanhas em tarefas é um **loop periódico** —
não é reativo a requisição nem a mensagem.

A pergunta: onde esse loop roda?

## Decisão

O dispatcher roda como **background task da API**, iniciada no `lifespan`:

```python
state.dispatcher_task = asyncio.create_task(dispatcher.run(), name="apt-dispatcher")
```

Não há container `scheduler` no `docker-compose.yml`. São **7 serviços em vez de 8**.

## Alternativas consideradas

**Container `scheduler` dedicado.** Foi o desenho original do plano desta entrega, e
é o mais correto em termos de separação de responsabilidades: um processo, uma
função. Recusado por custo/benefício num projeto de POC — mais um alvo de build, mais
um serviço no compose, mais um `Dockerfile` target e mais um ADR para justificar,
sem ganho arquitetural que a apresentação pudesse demonstrar. **A decisão foi tomada
explicitamente pela equipe** para reduzir a complexidade da entrega (ver
`docs/ATUALIZACOES-DOC-INICIAL.md`).

**Cron / `APScheduler` com persistência.** Um agendador com estado próprio,
disparando jobs. Recusado por resolver um problema que não temos: não precisamos de
agendamento por expressão cron nem de jobs persistidos. Precisamos de um tick de 1
segundo. `asyncio` já faz isso, e adicionar um agendador traria a sua própria
complexidade de estado.

**Materializar as tarefas no momento do `POST /campaigns`.** A alternativa mais
simples: criar as 10.000 tarefas de uma vez e publicá-las todas. Recusada por três
razões. Primeiro, a requisição HTTP levaria minutos. Segundo, e mais importante, o
**jitter deixaria de fazer sentido**: publicar tudo de uma vez transfere a
distribuição temporal para o rate limiter, e o que sairia da fila seria uma vazão
constante — regularidade perfeita, exatamente o que queremos evitar (ADR-004).
Terceiro, pausar a campanha não teria efeito, porque tudo já estaria enfileirado.

**Deixar o worker decidir quando enviar.** Sem scheduler: o worker consulta o banco,
descobre o que está devido e envia. Recusado por acoplar demais o worker: ele
passaria a precisar de lógica de jitter, de rotação do pool de URLs e de contabilidade
de orçamento — e cada réplica teria de coordenar com as outras para não duplicar. É
justamente a coordenação que o scheduler centralizado evita.

## O problema que essa escolha cria — e como foi resolvido

Se a API for escalada para várias réplicas, **cada uma rodaria o seu dispatcher** e
as campanhas seriam materializadas em duplicidade.

Isso está tratado. `CampaignRepository.claim_active_for_dispatch` usa
`SELECT ... FOR UPDATE SKIP LOCKED`:

```sql
SELECT ... FROM campaigns
WHERE status = 'active' AND dispatched_count < total_sends
ORDER BY updated_at ASC
LIMIT :limit
FOR UPDATE SKIP LOCKED
```

Duas réplicas nunca pegam a mesma campanha no mesmo tick — a segunda simplesmente
**pula** as linhas já travadas e trabalha nas outras. O lock vive apenas durante a
transação do tick, que dura milissegundos.

Como bônus, o `ORDER BY updated_at ASC` faz a campanha menos recentemente atendida
ser servida primeiro — um round-robin simples que impede uma campanha grande de
monopolizar todos os ticks.

## Consequências positivas

- **Um container a menos** para construir, subir e monitorar.
- **Reaproveita recursos já abertos.** O dispatcher usa o pool de conexões do
  Postgres e o publisher do RabbitMQ que a API já mantém. Num processo separado,
  seriam pools duplicados.
- **Ciclo de vida gerenciado.** O `lifespan` do FastAPI sobe o dispatcher no startup
  e o encerra ordenadamente no shutdown (`dispatcher.stop()` + `wait_for` com
  timeout de 5s).
- **Escala segura por construção.** O `SKIP LOCKED` torna múltiplas réplicas da API
  corretas, não apenas toleráveis.
- **O desacoplamento essencial permanece.** O que importa arquiteturalmente é
  API → fila → worker, e isso não mudou. O scheduler ficou do lado do produtor, que é
  onde ele conceitualmente pertence.

## Consequências negativas

- **Acoplamento de falha.** Um bug no loop do dispatcher pode degradar a latência da
  API — os dois compartilham o event loop. Mitigado com tratamento de exceção **por
  tick**: se um tick falha, o erro é logado e o loop continua. Sem isso, uma falha
  transitória do banco mataria a background task em silêncio e o sistema pararia de
  gerar tarefas sem nenhum erro visível (foi o comportamento observado na primeira
  versão, e é o motivo do `try/except` estar ali).
- **Não escala independentemente.** Se o scheduler precisasse de mais capacidade,
  seria necessário escalar a API junto. Não é um problema no volume da POC —
  materializar tarefas é barato comparado a enviá-las.
- **Menos observável.** As métricas do dispatcher se misturam às da API no mesmo
  `/metrics`. Mitigado pelo prefixo distinto (`apt_tasks_dispatched_total`).
- **Uma responsabilidade extra num processo.** É o custo conceitual: o serviço deixa
  de ser "a API" e passa a ser "a API e o scheduler". Está declarado no docstring de
  `src/apt/api/main.py`, para que ninguém descubra isso lendo o `docker-compose.yml`.

## Como validamos

- `GET /health/ready` inclui `dispatcher_ticks`, que comprova que o loop está vivo e
  avançando. É a verificação usada pelo `wait_for_stack` dos testes de carga.
- `tests/integration/test_api_campaigns.py` cria a app **sem** o lifespan
  (`create_app()` direto), de propósito: um scheduler materializando tarefas no meio
  do teste tornaria as contagens imprevisíveis. Isso confirma que a separação entre a
  API e o dispatcher é limpa o suficiente para testar uma sem a outra.
- O smoke test do CI verifica o caminho completo: a API cria a campanha, o
  dispatcher materializa, o worker envia. Se `total_accepted` ficar zero, o elo do
  scheduler está rompido e o build falha.
