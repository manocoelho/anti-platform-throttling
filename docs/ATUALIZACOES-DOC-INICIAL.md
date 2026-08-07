# O que mudou desde o Projeto 02 — e por quê

Documento exigido pela **Seção 8** do documento da disciplina: *"As equipes que escolherem
POCs sugeridas podem adaptá-las, adicionando ou removendo itens do escopo **com
justificativa documentada**."*

Registra todas as diferenças entre a arquitetura entregue no **Projeto 02** (Documentação
Inicial, 10/07/2026) e o **Projeto 03** (Documentação Final, 07/08/2026), com a razão de
cada uma.

## Resumo

| | Projeto 02 | Projeto 03 |
|---|---|---|
| Containers | 4 | **7** |
| Padrões arquiteturais | 3 | **6** (+1 bônus) |
| ADRs | 2 | **12** |
| Estado dos mecanismos | não especificado | **compartilhado no Redis** |
| Testes | "Pytest" | **119 unitários + 54 de integração\* + 3 cenários de carga** |
| Observabilidade | não prevista | **Prometheus, 11 métricas** |
| Diagramas C4 | níveis 1 e 2 | **níveis 1, 2 e 3** + topologia + sequência |

\* Dos 54 testes de integração: **44 passed, 1 failed, 9 skipped** — nunca leia como
"53 de 54 passando" ou qualquer soma que trate `skipped` como aprovação. Skip é por
design, quando falta uma infra específica que aquele teste exige; o único vermelho
(`test_pausa_e_retoma`) é um gap de fixture, não um bug de produção — detalhe completo
em RESULTADOS-TESTES.md § 1.5.

Nenhuma decisão do Projeto 02 foi **revertida**. As duas ADRs originais (RabbitMQ e
FastAPI) foram **confirmadas** pela implementação — e revisadas para refletir o que
aprendemos escrevendo o código.

---

## 1. Adições

### 1.1 Redis para o estado distribuído — a lacuna mais importante

**O Projeto 02 dizia:** *"Worker — aplicando as políticas de Rate Limiter, Circuit Breaker
e Retry"*, sem especificar onde o estado dessas políticas viveria.

**O que a implementação revelou.** A resposta a essa pergunta decide se o sistema funciona
ou não:

```
Rate limiter EM MEMÓRIA DE PROCESSO:
  1 worker  ×  3 req/s  =   3 req/s   ✓
  5 workers ×  3 req/s  =  15 req/s   ✗  3× o limite da plataforma
```

Cada worker respeitaria o seu limite local perfeitamente. O sistema violaria o limite da
plataforma em 3×. **O bug apareceria exatamente ao escalar** — sob carga, quando o limite
mais importa, e nunca em desenvolvimento com um worker.

O mesmo raciocínio vale para o circuit breaker: com estado local e threshold 5, cinco
workers precisariam de 25 requisições falhas antes do primeiro circuito abrir.

**A adição.** Redis como repositório do estado compartilhado de token buckets, circuit
breaker e feature flags, com decisões atômicas via scripts Lua.

**Registrado em:** [ADR-003](adr/ADR-003-redis-para-estado-distribuido.md),
[ADR-005](adr/ADR-005-script-lua-para-atomicidade.md),
[ADR-006](adr/ADR-006-circuit-breaker-distribuido.md).

### 1.2 Platform Simulator

**O Projeto 02 dizia:** *"plataformas externas (representadas por APIs simuladas)"* — sem
detalhar o que "simuladas" significava.

**Por que precisou de um serviço próprio.** Throttling é um comportamento **emergente**:
depende de quantas requisições chegam, com que espaçamento, dentro de qual janela. Um mock
que devolve 429 sob comando prova apenas que o nosso código **trata** 429 — não que o nosso
rate limiter **evita** receber 429. A diferença entre as duas afirmações é o projeto
inteiro.

**A decisão de projeto embutida.** O simulador usa **janela deslizante**; o nosso limiter
usa **token bucket**. A assimetria é deliberada: com o mesmo algoritmo nas duas pontas, o
teste provaria apenas que `3 < 5` — aritmética, não engenharia.

**Registrado em:** [ADR-008](adr/ADR-008-simulador-de-plataformas.md).

### 1.3 Prometheus e instrumentação

**O Projeto 02** não previa observabilidade.

**Por que entrou.** A Seção 8 do documento da disciplina indica que *"o uso de ferramentas
de observabilidade será considerado um diferencial"*. Mas a razão prática é outra: **sem
métricas, os testes de carga não têm o que medir**. O `peak_rps` observado pela plataforma,
a distribuição de envios entre réplicas e a latência p95 são a evidência da POC.

**O que ficou de fora.** O dashboard Grafana. A instrumentação está completa (11 métricas);
falta o painel visual. As consultas PromQL prontas para a demonstração estão em
[RESULTADOS-TESTES.md](RESULTADOS-TESTES.md#26-consultas-promql-para-a-demonstração).

### 1.4 Três padrões arquiteturais novos

| Padrão | Por que entrou |
|---|---|
| **Load Balancing** | Sai praticamente de graça — `prefetch=1` + competing consumers. E é o mecanismo que faz `--scale worker=5` funcionar, que é a demo central |
| **Bulkhead / Isolation** | Melhor relação valor/custo da lista. Sem ele, uma plataforma degradada derruba a vazão da outra — e de forma **silenciosa**, sem erro nenhum |
| **Feature Flag** | Baixo custo e função específica: o rate limiter e o breaker funcionam invisivelmente quando estão certos. As flags permitem desligá-los e mostrar o **contrafactual** |

Sobre o Feature Flag: sem o cenário "rate limiter desligado", o resultado "zero 429" não
significaria nada — não daria para distinguir "o rate limiter funcionou" de "a carga era
baixa".

### 1.5 Retry Pattern + DLQ

Não estava entre os padrões recomendados para a POC 4, mas consta na lista de
Confiabilidade (Seção 6.4) e completa o padrão de filas.

Duas decisões que valem registro: o tempo do backoff passa **dentro do broker** (filas de
TTL), não em `sleep()` no worker — com `prefetch=1`, um worker dormindo 30 s segura o seu
único slot; e **dois contadores separados** (`attempt` para falhas de envio, `defers` para
adiamentos nossos).

A separação dos contadores corrige um bug conceitual real: com contador único, uma tarefa
adiada 4× pelo rate limiter ia para a DLQ **sem nunca ter sido enviada**.

**Registrado em:** [ADR-009](adr/ADR-009-retry-com-filas-ttl.md).

### 1.6 Distribuição temporal (jitter)

**O Projeto 02** mencionava "distribuir a carga ao longo do tempo" na introdução, sem
mecanismo.

**Por que precisou de um módulo.** Respeitar o limite de vazão **não basta**. Um sistema
que envia exatamente 3 requisições no primeiro milissegundo de cada segundo respeita
3 req/s e exibe padrão obviamente automatizado: intervalos idênticos, variância zero. Os
mecanismos de detecção olham a **forma** da distribuição, não só o volume.

Três estratégias implementadas: `uniform`, `exponential` (processo de Poisson) e
`humanized` (exponencial modulada por perfil de atividade diário — o padrão).

---

## 2. Remoções

### 2.1 Traffic Sharding

**Recomendado pela POC 4. Removido.**

**Base normativa:** a Seção 2.2 exige **mínimo de 3** padrões; os 7 da POC 4 são "Padrões
**Recomendados**"; e o critério de avaliação mede *"uso **correto** dos padrões"*, não a
quantidade. Entregamos 6 — o dobro do mínimo.

**Justificativa:**

1. **O benefício se sobrepõe ao que já entregamos.** O objetivo — impedir que um conteúdo
   concentre volume — já é atingido pelo **eixo por conteúdo do rate limiter** (4 req/s por
   URL) e pela **rotação ponderada do pool de URLs**.
2. **Custo de defesa oral alto, ganho demonstrável baixo.** Hashing consistente com anel
   virtual e rebalanceamento é o conceito mais difícil da lista, e a demo seria sutil —
   mostrar que as chaves se distribuíram bem não produz o contraste das outras três demos.
3. **Complexidade estrutural desproporcional** para um cenário onde o número de partições
   nunca mudaria.

**O que perdemos, honestamente:** num sistema real com milhões de URLs, o sharding seria
necessário — o eixo por conteúdo cria uma chave Redis por URL, e isso não escala
indefinidamente (mitigado hoje por TTL de 1 h).

**Registrado em:** [ADR-011](adr/ADR-011-reducao-de-escopo-dos-padroes.md).

### 2.2 Terceira plataforma (TikTok)

O escopo da POC 4 menciona *"YouTube, Instagram, TikTok"*. Simulamos **duas**.

**Justificativa:** o TikTok não acrescentaria comportamento novo. O que o bulkhead precisa
demonstrar é isolamento entre plataformas de perfis **diferentes**, e dois limites
assimétricos (20 e 10 req/s) já produzem isso. Uma terceira plataforma seria configuração
duplicada, não capacidade nova.

### 2.3 Fila inteligente (deficit round-robin entre campanhas)

O escopo da POC 4 menciona *"fila inteligente: balanceamento de carga entre campanhas
ativas"*.

**Justificativa:** resolveria *starvation* entre campanhas concorrentes, cenário que a POC
não exercita com volume suficiente para gerar **evidência mensurável**. Código sem
validação não entra.

**Aproximação entregue:** o `ORDER BY updated_at ASC` do `claim_active_for_dispatch` serve
a campanha menos recentemente atendida primeiro — um round-robin simples que impede uma
campanha grande de monopolizar todos os ticks.

---

## 3. Mudanças estruturais

### 3.1 O scheduler não é um container separado

**Durante a implementação** ficou claro que faltava uma peça no desenho do Projeto 02:
alguém precisa transformar "mande 10.000 envios a 600/min" em tarefas individuais
publicadas na fila, espaçadas no tempo. Não é responsabilidade da API (que é reativa a
requisição) nem do worker (que é reativo a mensagem) — é um **loop periódico**.

**A decisão:** o dispatcher roda como **background task da API**, iniciada no `lifespan`.
São 7 serviços em vez de 8.

**Por quê:** um alvo de build a menos, e o dispatcher reaproveita o pool de conexões e o
publisher que a API já mantém. **A decisão foi tomada explicitamente pela equipe** para
reduzir a complexidade da entrega.

**O problema que isso cria, e como foi resolvido:** se a API for escalada, cada réplica
rodaria o seu dispatcher e as campanhas seriam materializadas em duplicidade. Tratado com
`SELECT ... FOR UPDATE SKIP LOCKED` — duas réplicas nunca pegam a mesma campanha no mesmo
tick.

**Registrado em:** [ADR-010](adr/ADR-010-scheduler-na-api.md).

### 3.2 Uma fila por plataforma, não uma fila única

**O Projeto 02** desenhava o RabbitMQ como um bloco único.

**A implementação** revelou que a fila dedicada por plataforma é a **camada estrutural do
Bulkhead**: mil tarefas de Instagram acumuladas não ficam à frente das de YouTube, porque
estão em outra fila. Com fila única compartilhada, a cabeça da fila seria um recurso
disputado.

### 3.3 Duas implementações de cada algoritmo distribuído

Token bucket e máquina de estados do breaker existem **em Python e em Lua**.

**Por quê:** a versão Lua é obrigatória (atomicidade); a Python entrega testabilidade (17
casos de borda em milissegundos, sem Docker) e é a *explicação legível* do algoritmo.

**O risco assumido:** duas fontes de verdade. Coberto por um **teste de paridade** que roda
a mesma sequência nas duas implementações e compara os resultados.

**Registrado em:** [ADR-005](adr/ADR-005-script-lua-para-atomicidade.md) e
[TRADE-OFFS](TRADE-OFFS.md#1-o-algoritmo-do-token-bucket-existe-duas-vezes).

### 3.4 SQL explícito em vez de ORM

**O Projeto 02** definiu PostgreSQL sem especificar a camada de acesso.

**A implementação** mostrou que as consultas que importam usam recursos específicos do
Postgres — `FOR UPDATE SKIP LOCKED`, `UPDATE ... RETURNING`, `ON CONFLICT DO UPDATE`,
`percentile_cont`, índices parciais — que no ORM virariam `session.execute(text(...))` de
qualquer forma.

**O custo assumido:** perdemos verificação de tipos das colunas (as linhas vêm como
`dict[str, Any]`).

**Registrado em:** [ADR-012](adr/ADR-012-sql-puro-em-vez-de-orm.md).

---

## 4. Revisão dos ADRs originais

Nenhuma decisão foi revertida. As duas foram **confirmadas** e as consequências
**reescritas** — o Projeto 02 as escreveu antes de haver código.

### ADR-001 — RabbitMQ

**Mantido.** Duas consequências positivas que o Projeto 02 não previa e que a
implementação revelou:

- **Filas dedicadas por plataforma** viabilizam o Bulkhead sem custo além de declarar mais
  uma fila.
- **DLX + TTL nativo** viabiliza o retry sem bloquear worker — recurso que não existiria
  numa fila implementada sobre tabela do Postgres.

Uma consequência negativa acrescentada: **semântica at-least-once permite duplicidade**. Se
um worker morre entre enviar e dar ack, a tarefa é reprocessada. É a limitação conhecida
mais séria do sistema.

### ADR-002 — FastAPI

**Mantido.** O Projeto 02 listava como consequência negativa *"ecossistema menor que
frameworks mais consolidados"*. Isso **não se materializou**: todas as bibliotecas de que
precisamos (`asyncpg`, `redis`, `aio-pika`, `httpx`, `prometheus-client`) são async-first e
independentes do framework web.

A consequência negativa que **de fato** apareceu é outra: **async é fácil de escrever e
difícil de acertar**. Uma única chamada bloqueante numa corrotina congela o event loop
inteiro — e não gera erro nenhum. O sintoma é latência alta sem causa aparente. É por isso
que o projeto não tem **nenhuma** dependência síncrona de I/O.

---

## 5. Correções de bugs encontrados durante a implementação

Sete bugs reais que valem contar na apresentação, porque são específicos deste domínio.
Os três primeiros (5.1–5.3) apareceram durante a implementação inicial. Os quatro
seguintes (5.4–5.7) só apareceram ao executar os testes de integração e os cenários de
carga pela primeira vez contra infraestrutura real — análise completa, evidência de
reprodução e os critérios de aceite afetados em
[RESULTADOS-TESTES.md](RESULTADOS-TESTES.md) e [TRADE-OFFS.md](TRADE-OFFS.md), itens
14–18.

### 5.1 O rate limiter abria o circuit breaker

**O sintoma.** Sob carga, o circuito abria sem que a plataforma tivesse devolvido nenhum
erro.

**A causa.** O adiamento do rate limiter estava sendo registrado como falha, e alimentava o
contador do breaker. Ou seja: **o rate limiter funcionando corretamente autobloqueava o
sistema**.

**A correção.** `Outcome` passou a separar `is_platform_rejection` (429, 5xx, timeout) de
`is_self_throttled` (rate limiter, bulkhead, circuito aberto). Só o primeiro grupo alimenta
o breaker, e há um teste garantindo que os dois grupos são **disjuntos**.

### 5.2 Tarefas iam para a DLQ sem nunca ter sido enviadas

**O sintoma.** A DLQ acumulava tarefas cujo `total_attempts` era 4, mas que não tinham
nenhuma execução com `outcome` de envio real.

**A causa.** Um contador único para falhas e adiamentos. Sob demanda acima do limite, os
adiamentos são frequentes e esperados — quatro deles esgotavam `max_attempts`.

**A correção.** Dois contadores: `attempt` (falhas de envio) e `defers` (adiamentos), com
teto próprio e generoso para o segundo. Testado em `test_defers_nao_incrementa_attempt`.

### 5.3 O dispatcher morria em silêncio

**O sintoma.** Campanhas ficavam `active` sem gerar nenhuma tarefa. Nenhum erro visível.

**A causa.** Uma exceção não tratada escapava do loop `run()` e matava a background task.
Como ninguém aguardava aquela task, a exceção não aparecia em lugar nenhum.

**A correção.** Tratamento de exceção **por tick**: se um tick falha, o erro é logado e o
loop continua. Uma falha transitória do banco deixou de matar o scheduler.

### 5.4 Calibração do burst deixava passar rajadas que estouravam o limite

**O sintoma.** 429 reais da plataforma mesmo com o rate limiter ligado, concentrados no
início de cada campanha.

**A causa.** `burst_capacity` (16 no YouTube) somado a `allowed_rps` (16) no mesmo
segundo superava o limite estimado (20) — a invariante testada só checava
`burst_capacity <= estimated_limit_rps`, que ignora o refill.

**A correção.** `burst_capacity + allowed_rps <= estimated_limit_rps`. Novos valores: 3
(YouTube) e 1 (Instagram) — um degrau abaixo do teto exato, para sobrar margem numa
demo ao vivo. Detalhe completo em [TRADE-OFFS.md, item 16](TRADE-OFFS.md).

### 5.5 Sonda do circuit breaker vazava e travava o circuito em half_open

**O sintoma.** No cenário de resiliência, o circuito do Instagram abria e sondava, mas
nunca fechava — ficava preso em `half_open` até o TTL do estado no Redis expirar.

**A causa.** Uma sonda admitida pelo breaker podia ser desviada pelo rate limiter antes
do envio. Como só `record_success`/`record_failure` liberavam o slot da sonda, e
nenhum dos dois era chamado nesse caminho, o slot ficava ocupado para sempre.

**A correção.** Nova operação `release` (Lua, `breaker_state.py` e
`CircuitBreaker.release_probe`), chamada pelo worker quando uma sonda admitida é
adiada por uma camada seguinte. Detalhe em [TRADE-OFFS.md, item 15](TRADE-OFFS.md).

### 5.6 Perda silenciosa de mensagens no retry

**O sintoma.** Tarefas adiadas (rate limiter, bulkhead, circuit breaker) desapareciam
sem chegar à DLQ nem à tabela `failures`.

**A causa.** A fila de retry preservava, ao dead-letter, a routing key `tier.N` (usada
para entrar na fila de espera), não a plataforma — `apt.tasks` não tinha binding para
`tier.N`, e a mensagem era descartada como inroteável.

**A correção.** Seis filas de retry (uma por plataforma × degrau, em vez de três
compartilhadas), com `x-dead-letter-routing-key` declarado explicitamente como a
própria plataforma. Adiada para uma rodada posterior de correções por ser estrutural
(a avaliação inicial era de que o risco de regressão não se justificava às vésperas de
uma demonstração ao vivo) — mas passou a ser necessária depois que a correção do item
5.5 abriu o caminho de adiamento que este bug destruía. Detalhe completo em
[TRADE-OFFS.md, item 14](TRADE-OFFS.md).

### 5.7 Dispatcher publicava antes do commit da transação

**O sintoma.** `ForeignKeyViolationError` esporádico no worker, mesmo quando o envio à
plataforma já tinha sido aceito — a tarefa desaparecia do registro como se tivesse
falhado.

**A causa.** O dispatcher publicava cada mensagem no RabbitMQ **dentro** da transação
que materializa o tick inteiro (até 200 tarefas), antes dela comitar. Um worker local
rápido podia consumir a mensagem antes de a linha em `send_tasks` estar visível para a
sua própria conexão.

**A correção.** As mensagens são coletadas durante o tick e só publicadas **depois**
que a transação comita. Descoberto durante a validação da correção do item 5.5 — sem
ele, boa parte das falhas reais nunca chegava a ser reportada ao circuit breaker.
Detalhe em [TRADE-OFFS.md, item 18](TRADE-OFFS.md).

---

## 6. Onde cada mudança está registrada

| Mudança | ADR |
|---|---|
| RabbitMQ (revisado) | [001](adr/ADR-001-rabbitmq-como-broker.md) |
| FastAPI (revisado) | [002](adr/ADR-002-fastapi-como-framework.md) |
| Redis para estado distribuído | [003](adr/ADR-003-redis-para-estado-distribuido.md) |
| Token bucket vs. janela deslizante | [004](adr/ADR-004-token-bucket-vs-sliding-window.md) |
| Script Lua para atomicidade | [005](adr/ADR-005-script-lua-para-atomicidade.md) |
| Circuit breaker distribuído | [006](adr/ADR-006-circuit-breaker-distribuido.md) |
| Bulkhead com filas dedicadas | [007](adr/ADR-007-bulkhead-com-filas-dedicadas.md) |
| Simulador de plataformas | [008](adr/ADR-008-simulador-de-plataformas.md) |
| Retry com filas de TTL | [009](adr/ADR-009-retry-com-filas-ttl.md) |
| Scheduler na API | [010](adr/ADR-010-scheduler-na-api.md) |
| **Redução de escopo dos padrões** | [011](adr/ADR-011-reducao-de-escopo-dos-padroes.md) |
| SQL explícito em vez de ORM | [012](adr/ADR-012-sql-puro-em-vez-de-orm.md) |

---

## 7. Pendências declaradas

Duas coisas que a equipe deve resolver antes ou durante a entrega final.

### 7.1 Tamanho da equipe

O documento da disciplina exige equipes de **5 integrantes** (Seções 1 e 2.1). A equipe tem
**4**. Isso não afeta a implementação, mas **precisa ser confirmado com o professor** antes
da entrega — e mencionado na apresentação, já que a avaliação é individual e o roteiro está
dividido em quatro partes.

### 7.2 Execução dos testes de carga

Os testes de integração e os três cenários de carga, resiliência e escala foram executados
numa VM Ubuntu 22.04 com Docker Engine nativo (a máquina de desenvolvimento original,
Windows + Docker Desktop/WSL2, tinha o runtime de containers quebrado). Números medidos,
critérios de aceite e análise de causa raiz para os itens que falharam estão em
[RESULTADOS-TESTES.md](RESULTADOS-TESTES.md).
