# ADR-006 — Circuit breaker distribuído, não por processo

**Status:** Aceita
**Origem:** Projeto 03 (o Projeto 02 previa o padrão, sem definir onde o estado ficaria)

## Contexto

O Projeto 02 listou "Circuit Breaker" entre os padrões, descrevendo-o como
"interrompe temporariamente o envio após erros consecutivos". A descrição está
correta e omite a decisão que importa: **quem conta esses erros?**

Praticamente toda biblioteca de circuit breaker mantém o estado em memória do
processo. Com 5 workers e `failure_threshold = 5`, isso significa:

```
worker 1: precisa ver 5 falhas para abrir o seu circuito
worker 2: precisa ver 5 falhas para abrir o seu circuito
...
worker 5: precisa ver 5 falhas para abrir o seu circuito
--------------------------------------------------------
25 requisições contra uma plataforma já em problema,
antes do primeiro circuito abrir
```

E há um agravante específico deste domínio. No caso de **throttling**, cada
requisição extra recebida durante a penalidade tende a **estender** a penalidade —
muitas plataformas renovam a janela de bloqueio a cada nova tentativa. Ou seja: o
circuit breaker por processo produziria exatamente o comportamento que a POC existe
para evitar.

Pior ainda: quando o primeiro worker finalmente abrir o circuito, os outros quatro
continuam martelando. O padrão só cumpre a função se a decisão for **coletiva**.

## Decisão

O estado do circuito vive no **Redis** (ADR-003), com um circuito **independente por
plataforma**, e as transições são atômicas via script Lua (ADR-005).

```
apt:cb:youtube    -> { state, failures, successes, opened_at, probes }
apt:cb:instagram  -> { state, failures, successes, opened_at, probes }
```

A quinta falha — vista por **qualquer** worker — abre o circuito para **todos**, de
uma vez.

A máquina de estados está documentada e testada em
`src/apt/resilience/breaker_state.py` (implementação pura de referência) e executada
em `src/apt/resilience/lua/circuit_breaker.lua`.

## Duas decisões de projeto embutidas

**Um circuito por plataforma, não um global.** É a junção com o Bulkhead (ADR-007):
quando o Instagram degrada, apenas o circuito dele abre e o YouTube segue enviando
na vazão normal. Um circuito único compartilhado transformaria a falha de uma
plataforma em parada total do sistema — a falha em cascata que o padrão existe para
impedir.

**Somente rejeições da PLATAFORMA contam como falha.** 429, 5xx e timeout contam
(`Outcome.is_platform_rejection`). Os adiamentos internos — rate limiter negou,
bulkhead cheio, circuito aberto — **não** contam
(`Outcome.is_self_throttled`).

Essa distinção não é detalhe: se o adiamento do rate limiter contasse como falha,
**o rate limiter funcionando corretamente abriria o circuit breaker**, e o sistema se
autobloquearia sem que a plataforma tivesse reclamado de nada. É o primeiro bug
conceitual que aparece ao juntar os dois padrões, e por isso os dois grupos de
`Outcome` são separados no domínio e verificados por teste.

## Alternativas consideradas

**Breaker por processo (biblioteca pronta, ex. `pybreaker` ou `purgatory`).**
Descartado pelo raciocínio do Contexto: `threshold × N_workers` falhas antes de
qualquer proteção, e proteção parcial depois disso.

**Breaker por processo com threshold dividido (`threshold / N_workers`).** Uma
tentativa de contornar o problema sem estado compartilhado: se são 5 workers, cada
um abre com 1 falha. Recusado por ser frágil de duas formas. Primeiro, `N_workers`
passaria a ser configuração acoplada ao número de réplicas — escalar exigiria
recalcular o threshold. Segundo, e mais grave: falhas isoladas passariam a abrir o
circuito, porque o gatilho deixaria de ser "N falhas consecutivas" e viraria "1
falha". Falhas isoladas fazem parte da vida de qualquer chamada de rede.

**Um serviço centralizado de circuit breaker.** Um processo mantendo o estado e
respondendo consultas. Recusado: seria reimplementar o Redis com pior desempenho e
sem persistência, e o serviço viraria um SPOF adicional.

**Detectar a falha no scheduler e pausar as campanhas.** Uma abordagem
completamente diferente: em vez de barrar no worker, parar de materializar tarefas.
Recusada como mecanismo *principal* porque age tarde demais — as tarefas já
enfileiradas continuariam a ser enviadas. Existe no projeto, mas como recurso
**complementar** e opcional: a feature flag `auto_pause_on_open` (desligada por
padrão, porque com ela ativa o teste de resiliência pararia de gerar tráfego e não
daria para observar a recuperação).

## Consequências positivas

- **Reação coletiva e rápida.** `failure_threshold` falhas no total, não por
  processo. O ganho é proporcional ao número de réplicas.
- **Isolamento entre plataformas.** Uma degradada não afeta a outra.
- **A cota de sondas de `half_open` é respeitada de verdade.** Sem atomicidade,
  três workers passariam por um limite de 2 e a "sonda" viraria uma rajada sobre um
  serviço recém-recuperado.
- **Estado inspecionável.** `GET /platforms` mostra o circuito ao vivo, e
  `GET /admin/breaker-events` traz o histórico de transições persistido no Postgres —
  a evidência usada no teste de resiliência.
- **Um worker novo já nasce sabendo.** Uma réplica que sobe durante um incidente
  encontra o circuito aberto na primeira consulta, sem ter de aprender do zero.

## Consequências negativas

- **Uma ida ao Redis por envio.** Está no caminho crítico. Mitigado pela ordem das
  camadas no worker: o breaker é consultado **antes** do rate limiter (que custa
  duas idas), então um circuito aberto economiza as consultas seguintes.
- **Fail-open quando o Redis cai.** Sem Redis, permitimos o envio. Menos arriscado
  do que parece: se a plataforma realmente estiver com problema, os 429/5xx continuam
  chegando, o retry com backoff continua espaçando as tentativas e o bulkhead continua
  limitando a concorrência. Perdemos a *antecipação*, não todas as defesas.
- **Respostas atrasadas exigem tratamento explícito.** Um envio em voo no momento em
  que o circuito abre pode responder depois. Decidimos que: sucesso atrasado **não**
  fecha o circuito (é informação mais antiga que a decisão de abrir) e falha atrasada
  **não** reinicia o cooldown (reiniciar a cada resposta tardia poderia manter o
  circuito aberto para sempre, mesmo depois da plataforma voltar). Os dois casos são
  testados.
- **Máquina de estados duplicada em Lua e Python.** Mesmo custo do ADR-005, mesma
  mitigação.

## Como validamos

- **`tests/integration/test_circuit_breaker_redis.py::TestEstadoCompartilhado::test_falhas_contadas_coletivamente`**
  — cinco instâncias distintas de `CircuitBreaker` (`observer_id` diferente,
  simulando cinco processos), **uma falha cada**, e um sexto worker que nunca viu
  falha nenhuma já encontra o circuito aberto. É a validação direta desta decisão.
- `test_circuitos_por_plataforma_sao_independentes` — abrir o Instagram não afeta o
  YouTube.
- `tests/unit/test_breaker_state.py` — 11 casos cobrindo as transições, incluindo
  sucesso zerando o contador, falha em `half_open` reabrindo, e os dois casos de
  resposta atrasada.
- `tests/unit/test_domain.py::TestOutcome::test_autolimitacao_nao_conta_como_rejeicao`
  — garante que adiamentos internos nunca alimentam o breaker.
- `tests/load/resilience_test.py` — o ciclo completo de ponta a ponta:
  `closed → open → half_open → closed`, com o YouTube seguindo enquanto o
  Instagram está fora.
