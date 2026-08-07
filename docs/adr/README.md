# Architecture Decision Records (ADRs)

Um ADR registra uma decisão arquitetural com o **contexto** em que foi tomada, as
**alternativas** consideradas e as **consequências** — inclusive as negativas.

O objetivo não é justificar a decisão, é preservar o raciocínio. Seis meses
depois, quando alguém perguntar "por que isso está em Lua?", a resposta está aqui
e não na memória de quem escreveu.

## Índice

| ADR | Decisão | Status | Origem |
|---|---|---|---|
| [ADR-001](ADR-001-rabbitmq-como-broker.md) | RabbitMQ como message broker | Aceita | Projeto 02, **revisado** |
| [ADR-002](ADR-002-fastapi-como-framework.md) | FastAPI como framework backend | Aceita | Projeto 02, **revisado** |
| [ADR-003](ADR-003-redis-para-estado-distribuido.md) | Redis para o estado distribuído | Aceita | Projeto 03 |
| [ADR-004](ADR-004-token-bucket-vs-sliding-window.md) | Token bucket em vez de janela deslizante | Aceita | Projeto 03 |
| [ADR-005](ADR-005-script-lua-para-atomicidade.md) | Script Lua para garantir atomicidade | Aceita | Projeto 03 |
| [ADR-006](ADR-006-circuit-breaker-distribuido.md) | Circuit breaker distribuído, não por processo | Aceita | Projeto 03 |
| [ADR-007](ADR-007-bulkhead-com-filas-dedicadas.md) | Bulkhead com filas e pools dedicados | Aceita | Projeto 03 |
| [ADR-008](ADR-008-simulador-de-plataformas.md) | Simulador próprio em vez de mocks ou APIs reais | Aceita | Projeto 03 |
| [ADR-009](ADR-009-retry-com-filas-ttl.md) | Retry com filas de TTL, não `sleep` no worker | Aceita | Projeto 03 |
| [ADR-010](ADR-010-scheduler-na-api.md) | Scheduler como background task da API | Aceita | Projeto 03 |
| [ADR-011](ADR-011-reducao-de-escopo-dos-padroes.md) | Redução de 7 para 6 padrões arquiteturais | Aceita | Projeto 03 |
| [ADR-012](ADR-012-sql-puro-em-vez-de-orm.md) | SQL explícito em vez de ORM | Aceita | Projeto 03 |

## Como ler

Os ADRs **001** e **002** vieram do Projeto 02 e foram **revisados** nesta
entrega: a decisão se manteve, mas as consequências foram reescritas com o que
aprendemos implementando (o Projeto 02 as escreveu antes de haver código).

Os ADRs **003 a 012** são novos. Os mais importantes para entender o sistema são:

- **ADR-005** — é a decisão que sustenta a tese central da POC. Sem atomicidade,
  o rate limiter falha exatamente ao escalar.
- **ADR-011** — registra por escrito a redução de escopo, exigida pela Seção 8 do
  documento da disciplina.

## Formato

Todos seguem a mesma estrutura:

```
Status · Contexto · Decisão · Alternativas consideradas ·
Consequências positivas · Consequências negativas · Como validamos
```

A seção **Como validamos** não é padrão em ADRs, e foi adicionada de propósito:
ela aponta o teste ou a medição que comprova que a decisão funciona. Uma decisão
sem verificação é uma opinião.
