# ADR-011 — Redução de 7 para 6 padrões arquiteturais

**Status:** Aceita
**Origem:** Projeto 03

> Este ADR existe para cumprir a exigência da **Seção 8** do documento da
> disciplina: *"As equipes que escolherem POCs sugeridas podem adaptá-las,
> adicionando ou removendo itens do escopo **com justificativa documentada**."*
>
> Registra também a base normativa da decisão: a Seção 2.2 exige **mínimo de 3**
> padrões, os 7 da POC 4 são "Padrões **Recomendados**", e o critério de avaliação
> (30%) mede *"uso **correto** dos padrões"* — não a quantidade.

## Contexto

O documento da disciplina recomenda sete padrões para a POC 4:

Rate Limit/Throttling · Load Balancing · Queues/PubSub/Fanout · **Traffic
Sharding** · Bulkhead/Isolation · Circuit Breaker · Feature Flag

O Projeto 02 da equipe cobria três (Producer-Consumer, Rate Limiter, Circuit
Breaker). Ao planejar esta entrega, avaliamos cada padrão recomendado em três
eixos: **custo de implementação**, **valor demonstrável** e — o que acabou sendo
decisivo — **facilidade de defesa oral pela equipe na apresentação**.

O terceiro eixo merece explicação. O critério de nota do Videocast (20%) inclui
"domínio técnico", e a avaliação é individual. Um padrão que os quatro integrantes
não conseguem explicar em 90 segundos cada é um passivo, não um ativo: aparece no
repositório, aumenta a superfície de perguntas e não sustenta a defesa.

## Decisão

Implementamos **6 padrões** e removemos **Traffic Sharding**.

| Padrão | Situação | Onde está |
|---|---|---|
| Rate Limit / Throttling | **implementado** | `resilience/rate_limiter.py` + `lua/token_bucket.lua` |
| Circuit Breaker | **implementado** | `resilience/circuit_breaker.py` + `lua/circuit_breaker.lua` |
| Queues / PubSub / Fanout | **implementado** | `messaging/topology.py` |
| Load Balancing | **implementado** | `messaging/consumer.py` (competing consumers, `prefetch=1`) |
| Bulkhead / Isolation | **implementado** | `resilience/bulkhead.py` + filas dedicadas |
| Feature Flag | **implementado** | `resilience/feature_flags.py` + `api/routers/flags.py` |
| **Traffic Sharding** | **removido** | — |

Além dos seis, o projeto entrega **Retry Pattern + DLQ** (ADR-009), que consta na
lista de Confiabilidade da Seção 6.4 do documento e não estava entre os
recomendados para a POC 4.

## Justificativa da remoção do Traffic Sharding

**O que ele resolveria.** Distribuir o tráfego entre partições por hashing
consistente sobre uma chave (no nosso caso, a URL do conteúdo), de forma que um
conteúdo "quente" não afogue os outros e que o rebalanceamento ao adicionar
partições mova o mínimo de chaves possível.

**Por que removemos:**

1. **O benefício se sobrepõe ao que já entregamos.** O objetivo — impedir que um
   conteúdo concentre volume — já é atingido pelo **eixo por conteúdo do rate
   limiter** (`content_bucket_key`, 4 req/s por URL) e pela **rotação ponderada do
   pool de URLs** (`ContentRepository.take_next`). Com 2 plataformas e um volume
   controlado, o sharding adicionaria uma segunda camada resolvendo o mesmo problema.

2. **O custo de defesa oral é alto e o ganho demonstrável é baixo.** Hashing
   consistente com anel virtual e rebalanceamento é o conceito mais difícil da lista.
   E a demo seria sutil: mostrar que as chaves se distribuíram bem entre partições não
   produz o contraste visual que as outras demos produzem (subir 5 workers e a vazão
   não aumentar; matar uma plataforma e a outra seguir).

3. **Complexidade estrutural desproporcional.** Exigiria filas por
   `plataforma × shard`, decisão de roteamento no dispatcher, e uma estratégia de
   rebalanceamento — para um cenário onde o número de partições nunca mudaria.

**O que perdemos, honestamente:** num sistema real com centenas de plataformas e
milhões de URLs, o sharding seria necessário — o eixo por conteúdo do rate limiter
cria uma chave Redis por URL, e isso não escala indefinidamente (mitigado hoje pelo
TTL das chaves). Está registrado como evolução natural em `docs/TRADE-OFFS.md`.

## Também fora do escopo (decisões relacionadas)

**Fila inteligente / deficit round-robin entre campanhas.** Resolveria
*starvation* entre campanhas concorrentes disputando a mesma plataforma. Removido
porque a POC não exercita esse cenário com volume suficiente para gerar evidência
mensurável — e sem medição, teríamos código sem validação. Uma aproximação simples
existe: o `ORDER BY updated_at ASC` do `claim_active_for_dispatch` serve a campanha
menos recentemente atendida primeiro.

**Idempotência ponta a ponta.** Não estava entre os recomendados para a POC 4 (é da
POC 3). Foi considerada porque a semântica at-least-once do RabbitMQ permite envio
duplicado, e removida por escopo. Registrada em `docs/TRADE-OFFS.md` como a
limitação conhecida mais relevante do sistema.

**Simulação de 3 plataformas (TikTok).** Reduzido para 2 (YouTube e Instagram). O
TikTok não acrescentaria comportamento novo — o que o bulkhead precisa demonstrar é
isolamento entre plataformas de perfis **diferentes**, e dois limites assimétricos
(20 e 10 req/s) já produzem isso.

**Dashboard Grafana.** Reduzido a Prometheus + `/metrics`. A instrumentação está
completa (11 métricas); o que ficou de fora é o painel visual. As consultas PromQL
prontas para a demo estão em `docs/RESULTADOS-TESTES.md`.

## Consequências positivas

- **Seis padroes bem implementados e testados**, em vez de sete com um deles
  superficial. Alinhado ao critério "uso **correto** dos padrões".
- **Cada padrão tem uma demonstração ao vivo** e um teste que o valida.
- **A equipe consegue defender todo o código.** Nenhum mecanismo entrou no
  repositório sem que houvesse quem soubesse explicá-lo.
- **Duas vezes o mínimo exigido** (6 contra 3 da Seção 2.2).

## Consequências negativas

- **Um padrão recomendado a menos** que a lista da POC 4. É uma escolha visível, e
  é por isso que existe este documento — a remoção precisa aparecer como decisão
  justificada, não como omissão.
- **O eixo por conteúdo do rate limiter não escala indefinidamente.** Uma chave
  Redis por URL, mitigada por TTL de 1h. Com milhões de URLs ativas, o sharding
  seria necessário.
- **Sem evidência de comportamento sob starvation entre campanhas.** Não medimos
  esse cenário, e portanto não afirmamos nada sobre ele.

## Como validamos

O critério de aceite desta decisão não é um teste automatizado, e sim a cobertura:
cada um dos seis padrões mantidos tem (a) uma seção em `docs/PADROES.md` apontando o
arquivo, (b) pelo menos um teste que verifica a sua propriedade central, e (c) uma
demonstração executável em `docs/ROTEIRO-APRESENTACAO.md`.

| Padrão | Teste que valida a propriedade central |
|---|---|
| Rate Limit | `test_rate_limiter_redis.py::TestConcorrencia` + `tests/load/scale_test.py` |
| Circuit Breaker | `test_circuit_breaker_redis.py::TestEstadoCompartilhado` |
| Queues + DLQ | `test_messaging.py::TestRetryComTTL` |
| Load Balancing | `tests/load/scale_test.py` (distribuição entre réplicas) |
| Bulkhead | `test_bulkhead.py::TestIsolamento` + `tests/load/resilience_test.py` (H2) |
| Feature Flag | `test_messaging.py::TestFanoutDeControle` + `tests/load/load_test.py` |
