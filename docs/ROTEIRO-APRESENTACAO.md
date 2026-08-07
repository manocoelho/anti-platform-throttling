# Roteiro da apresentação — 12 a 15 min

O documento da disciplina é rigoroso: **mínimo 12, máximo 15 minutos**, com **participação
de todos os integrantes**, e a nota do Videocast (20%) avalia *"clareza da apresentação,
participação de todos, demonstração funcional e domínio técnico"*.

**A avaliação é individual.** Cada um responde pelo que apresenta — o roteiro dá a cada
integrante o bloco que corresponde ao que ele implementou (ver
[PRS-SUGERIDOS.md](PRS-SUGERIDOS.md)).

## Divisão do tempo

| Bloco | Quem | Tempo | Acumulado |
|---|---|---|---|
| 1. Contexto e problema | **Alisson** | 2:00 | 2:00 |
| 2. Arquitetura e a decisão central | **Cássio** | 3:00 | 5:00 |
| 3. Os padrões e a mensageria | **Antônio** | 3:00 | 8:00 |
| 4. Demonstração ao vivo | **João Vitor** | 4:00 | 12:00 |
| 5. Resultados e lições aprendidas | **todos** | 1:30 | 13:30 |

Sobra ~1:30 de margem. Use-a: apresentação que estoura 15 min perde nota por critério
explícito.

---

## Bloco 1 — Contexto e problema · Alisson · 2:00

**Objetivo:** deixar claro que o problema não é óbvio.

### O que dizer

> "Sistemas distribuídos modernos dependem de APIs de terceiros que impõem limites de
> utilização. Esses limites **não são publicados** e mudam sem aviso. Exceder causa
> throttling e, na reincidência, penalização algorítmica."

**O ponto que torna o problema interessante** — e vale gastar 20 segundos nele:

> "E há um agravante: em muitas plataformas, cada requisição enviada **durante** a
> penalidade **estende** a penalidade. Ou seja, a reação intuitiva — tentar de novo — é
> exatamente a pior."

### Feche com a pergunta que organiza a apresentação

> "Como enviar volume alto respeitando um limite que você **não conhece**?"
>
> "Três coisas: manter a vazão abaixo de uma estimativa com **margem de segurança**;
> **distribuir** os envios no tempo para não parecer máquina; e **parar de tentar** quando a
> plataforma reclamar."

### Honestidade que ganha crédito (15 s)

> "Uma ressalva metodológica: nós **não** descobrimos os limites reais do YouTube ou do
> Instagram. Fazer isso exigiria enviar tráfego artificial a APIs de terceiros — teste de
> carga não autorizado e violação de termos de uso. O que validamos é o **mecanismo** de
> respeitar um limite desconhecido. Os números do nosso simulador são estimativas
> declaradas como tal, e isso está no ADR-008."

**Slide:** o diagrama C4 nível 1 ([ARQUITETURA.md](ARQUITETURA.md#nível-1--contexto)).

---

## Bloco 2 — Arquitetura e a decisão central · Cássio · 3:00

**Objetivo:** apresentar os 7 containers e chegar à decisão que sustenta o projeto.

### Parte A — os containers (1:00)

Percorra o C4 nível 2. Não descreva cada caixa — agrupe:

> "Sete serviços. A API recebe e, no mesmo processo, o scheduler materializa campanhas em
> tarefas. O RabbitMQ desacopla. N workers consomem e aplicam as políticas. O simulador faz
> o papel das plataformas. Postgres persiste, Prometheus observa."
>
> "E o Redis — que é o serviço que não estava no nosso Projeto 02, e é o mais importante."

### Parte B — a decisão central (2:00)

**Este é o momento mais importante da apresentação.** Não apresse.

> "No Projeto 02 escrevemos que o worker aplicaria rate limiter e circuit breaker. Não
> especificamos **onde o estado deles ficaria** — e essa é a decisão que define se o
> sistema funciona."

Escreva no slide, e leia em voz alta:

```
Rate limiter EM MEMÓRIA DE PROCESSO:
  1 worker  × 16 req/s  =  16 req/s   ✓
  5 workers × 16 req/s  =  80 req/s   ✗   4× o limite da plataforma
```

> "Cada worker respeitaria o seu limite perfeitamente. O sistema violaria o limite da
> plataforma em quatro vezes."
>
> "E note **quando** esse bug apareceria: só ao escalar. Em desenvolvimento, com um worker,
> o código parece correto. O bug se manifesta em produção, sob carga — no pior momento
> possível."

**A solução, e a razão do Lua** (60 s):

> "O estado vive no Redis. Mas guardar no Redis não basta: a versão ingênua faz `GET`,
> decide, `SET` — e entre a leitura e a escrita existe uma janela. Cinco workers podem ler
> 'resta uma ficha' ao mesmo tempo, todos concluírem que podem enviar, e cinco requisições
> saírem."
>
> "Por isso a decisão roda **dentro** do Redis, num script Lua. O Redis executa scripts
> atomicamente — enquanto ele roda, nenhum outro comando é processado. A janela deixa de
> existir."
>
> "Temos um teste com 50 corrotinas simultâneas disputando um balde de capacidade 10.
> Passam exatamente 10. Com read-modify-write, esse teste falharia."

**Se perguntarem "por que não `WATCH`/`MULTI`?"**

> "Funcionaria, mas sob alta contenção — que é o nosso caso, todos os workers na mesma
> chave — a taxa de retry do `EXEC` dispararia. O Lua resolve numa ida à rede, sempre."

---

## Bloco 3 — Os padrões e a mensageria · Antônio · 3:00

**Objetivo:** mostrar os 6 padrões e como eles se compõem.

### Abra justificando o número (20 s)

> "Seis padrões. A POC 4 recomenda sete — removemos o Traffic Sharding com justificativa
> documentada, como permite a Seção 8 do documento. O mínimo exigido são três. O benefício
> dele se sobrepunha ao que o eixo por conteúdo do nosso rate limiter já entrega."

### As cinco camadas do worker (1:20)

Mostre o C4 nível 3 e percorra a ordem:

```
1. Feature flags    cache local        ~zero
2. Bulkhead         semáforo local     memória
3. Circuit breaker  1 ida ao Redis
4. Rate limiter     2 idas ao Redis
5. Envio            chamada de rede    o mais caro
```

> "A ordem não é arbitrária: cada camada é mais barata que a seguinte, e recusar cedo evita
> gastar o recurso da próxima."

**O detalhe que demonstra domínio:**

> "O breaker vem **antes** do rate limiter. Se fosse o contrário, consumiríamos uma ficha do
> balde para depois descobrir que o circuito está aberto — e a ficha **não volta**. Sob
> carga, esse vazamento reduziria a vazão efetiva abaixo do configurado."

### Bulkhead — três camadas (40 s)

> "Isolamos por plataforma em três níveis: **fila dedicada** no broker, **semáforo** nos
> slots de execução, e **pool HTTP** nas conexões. As três são necessárias — cada uma
> sozinha deixa um recurso compartilhado."
>
> "Sem isso, o Instagram respondendo em 5 segundos ocuparia todos os slots e a vazão do
> YouTube despencaria. E o pior: **sem nenhum erro aparecer**. Quem investigasse ia olhar o
> YouTube primeiro e não encontrar nada."

### A mensageria (40 s)

> "Topic exchange rotea por plataforma. Fanout difunde eventos de controle — precisa ser
> fanout, porque uma invalidação de feature flag tem de chegar a **todos** os workers; com
> topic, chegaria a um só."
>
> "E o backoff do retry passa **dentro do broker**, em filas com TTL. Não usamos `sleep` no
> worker: com `prefetch=1`, um worker dormindo 30 segundos segura o seu único slot e para de
> consumir."

### O bug que vale contar (20 s)

> "Um erro real que cometemos: tínhamos um contador único para falhas e adiamentos. Sob
> carga, quatro adiamentos do rate limiter esgotavam as tentativas e a tarefa ia para a DLQ
> **sem nunca ter sido enviada**. O sistema descartava trabalho legítimo justamente quando
> estava se protegendo corretamente. Separamos em dois contadores."

---

## Bloco 4 — Demonstração ao vivo · João Vitor · 4:00

**Objetivo:** provar as três propriedades.

> ### Prepare antes de gravar
> ```bash
> docker compose up -d --build && docker compose ps   # todos healthy
> curl -X POST localhost:8000/admin/reset/rate-limiter
> curl -X POST localhost:8000/admin/reset/circuit-breaker
> curl -X POST localhost:9001/admin/reset
> curl -X POST localhost:8000/flags/reset
> ```
> Deixe abertos: Prometheus (9090), RabbitMQ (15672) e dois terminais.
>
> **Se a demo ao vivo falhar durante a gravação, use a gravada.** O documento permite
> "demo ao vivo ou gravada" — e uma demo travada consome os 4 minutos sem provar nada.

### Demo 1 — Escalar não aumenta a vazão (1:40)

**A demo mais importante.**

```bash
# 1 worker
docker compose up -d --scale worker=1
curl -X POST localhost:8000/campaigns -H 'Content-Type: application/json' \
     -d @examples/campaign.json
sleep 25
curl -s localhost:9001/admin/stats | python -m json.tool     # anote peak_rps

# 5 workers, mesma carga
curl -X POST localhost:8000/admin/reset/rate-limiter
curl -X POST localhost:9001/admin/reset
docker compose up -d --scale worker=5
curl -X POST localhost:8000/campaigns -H 'Content-Type: application/json' \
     -d @examples/campaign.json
sleep 25
curl -s localhost:9001/admin/stats | python -m json.tool     # peak_rps IGUAL
```

**A frase que fecha:**

> "Cinco vezes mais workers, **mesmo** pico observado pela plataforma. Com rate limiter em
> memória de processo, esse número teria sido cinco vezes maior."

Complete com o load balancing:

```bash
curl -s localhost:8000/admin/workers | python -m json.tool
```

> "E a carga foi distribuída entre as cinco réplicas — `prefetch=1` com competing
> consumers."

### Demo 2 — Uma plataforma cai, a outra segue (1:20)

```bash
curl -X POST localhost:9001/admin/fault -H 'Content-Type: application/json' \
     -d '{"platform":"instagram","mode":"error_500","ttl_seconds":25}'

# acompanhe: o circuito do Instagram abre, o do YouTube não
watch -n 2 'curl -s localhost:8000/platforms | python -m json.tool | grep -E "platform|circuit"'
```

Espere a falha expirar (25 s) e mostre a recuperação:

```bash
curl -s localhost:8000/admin/breaker-events | python -m json.tool
```

> "`closed → open`, depois `open → half_open → closed`. O circuito **sondou** a recuperação
> com um número limitado de requisições e fechou sozinho. E o YouTube nunca parou — é o
> bulkhead."

### Demo 3 — O contrafactual (1:00)

```bash
curl -X PATCH localhost:8000/flags/jitter_enabled \
     -H 'Content-Type: application/json' -d '{"value":false}'
sleep 20
curl -s localhost:9001/admin/stats | python -m json.tool
```

> "Desligamos o jitter. Os envios passaram a sair em rajada no início de cada tick, e os
> 429 apareceram."
>
> "Essa demo é o **controle** do experimento. Sem ela, 'zero 429' não significaria nada —
> não daria para distinguir 'o mecanismo funcionou' de 'a carga era baixa'."

---

## Bloco 5 — Resultados e lições · todos · 1:30

Cada integrante fala 20 s. Distribua assim:

### Cássio — os números

> "117 testes unitários em 2 segundos, sem infraestrutura nenhuma. Lint, formatação e mypy
> estrito limpos. Os módulos que sustentam a tese — token bucket, breaker, jitter, retry —
> estão entre 86% e 100% de cobertura."

### Alisson — a decisão que possibilitou isso

> "E isso não foi sorte. Escrevemos a lógica crítica como **função pura**: sem I/O,
> recebendo o tempo como parâmetro. É o que permite testar 'o circuito abre após 15
> segundos' **sem esperar 15 segundos**, e rodar tudo no CI sem levantar container."

### Antônio — a lição sobre padrões

> "A lição principal: os padrões **interagem**, e a interação é onde estão os bugs. Rate
> limiter e circuit breaker isolados são simples. Juntos, o adiamento de um alimentava o
> contador do outro — e o rate limiter abria o circuito ao fazer o próprio trabalho."

### João Vitor — a limitação e o fechamento

> "E a nossa limitação mais séria, declarada: falta idempotência ponta a ponta. A semântica
> at-least-once do RabbitMQ permite envio duplicado se um worker morrer entre enviar e dar
> ack. Está no TRADE-OFFS, item 3, com o caminho para resolver."
>
> "Escolhemos at-least-once porque duplicar é recuperável e perder não é. Mas é uma escolha,
> não uma solução."

---

## Perguntas prováveis da banca

Respostas curtas. As longas estão em
[CODIGO-EXPLICADO.md § Q&A](CODIGO-EXPLICADO.md#11-qa-geral).

| Pergunta | Quem responde | Resposta em uma frase |
|---|---|---|
| "Esses limites são os reais das plataformas?" | Alisson | Não — estimativas declaradas como tal; validamos o mecanismo, não os números (ADR-008). |
| "Por que Lua e não `INCR`?" | Cássio | `INCR` não sabe fazer refill por tempo, nem limitar à capacidade, nem calcular o prazo até a próxima ficha. |
| "E se o Redis cair?" | Cássio | Fail-open: permitimos o envio e logamos em ERROR. Fail-closed viraria indisponibilidade total; as outras camadas seguem ativas. |
| "Por que 6 padrões e não 7?" | Antônio | Traffic Sharding removido com justificativa (ADR-011); o mínimo exigido são 3 e o critério avalia uso correto, não quantidade. |
| "Por que o scheduler está dentro da API?" | Antônio | Decisão explícita de reduzir complexidade; a duplicidade em múltiplas réplicas está tratada com `SKIP LOCKED` (ADR-010). |
| "Como garantem que Lua e Python não divergem?" | João Vitor | Teste de paridade: 30 passos da mesma sequência nas duas, comparando os três valores de retorno. |
| "Por que o algoritmo do simulador é diferente?" | João Vitor | Deliberado. Com o mesmo algoritmo nas duas pontas, o teste provaria apenas que 16 < 20. |
| "Qual a maior limitação?" | João Vitor | Falta idempotência; at-least-once permite envio duplicado. |
| "Onde vocês erraram?" | Antônio | Três bugs contados na apresentação: contador único, dispatcher morrendo em silêncio, e `depends_on` sem healthcheck. |

---

## Checklist antes de gravar

- [ ] `docker compose ps` — todos os 7 serviços **healthy**
- [ ] Estado resetado (rate limiter, breaker, simulador, flags)
- [ ] Prometheus e RabbitMQ abertos em abas
- [ ] Terminal com fonte grande o suficiente para ler no vídeo
- [ ] Cronômetro visível — o limite de 15 min é rigoroso
- [ ] **Todos os 4 integrantes falam** (critério explícito)
- [ ] Link do vídeo inserido no README
- [ ] Demo gravada como reserva, caso a ao vivo falhe

## Checklist da entrega final (Seção 7 do documento)

| ✔ | Item | Onde |
|---|---|---|
| ☑ | Repositório Git acessível | — |
| ☑ | README com descrição, execução e link do videocast | [README.md](../README.md) — **falta o link** |
| ☑ | Diagrama C4 (níveis 1 e 2, no mínimo) | [ARQUITETURA.md](ARQUITETURA.md) — níveis 1, 2 **e 3** |
| ☑ | ADRs completos | [adr/](adr/) — 12 ADRs |
| ☑ | Código funcional com testes | 117 unitários + 4 integração + 3 cenários |
| ☑ | Docker Compose para execução local | [docker-compose.yml](../docker-compose.yml) |
| ☐ | **Videocast gravado com todos os integrantes** | a gravar |
| ☐ | **Link do videocast no README** | a inserir |
| ☐ | Histórico de commits de todos os membros | [PRS-SUGERIDOS.md](PRS-SUGERIDOS.md) — 48 PRs atribuídos |
| ☐ | **Plano e resultados de testes** | plano ✅ · resultados **parciais** (ver [RESULTADOS-TESTES.md](RESULTADOS-TESTES.md) Parte 2) |
| ☑ | Trade-offs documentados | [TRADE-OFFS.md](TRADE-OFFS.md) — 13 itens |
| ☑ | **Seção "Ferramentas de IA utilizadas"** | [README.md](../README.md) — **omissão desclassifica** |

> **Atenção aos três itens em aberto.** O videocast e o link são obrigatórios. Os resultados
> de carga precisam ser executados numa máquina com Docker funcional — os testes estão
> implementados e imprimem o relatório pronto para colar.
