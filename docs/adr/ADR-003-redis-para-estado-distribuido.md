# ADR-003 — Redis para o estado distribuído

**Status:** Aceita
**Origem:** Projeto 03 (não existia no Projeto 02)

## Contexto

Este ADR corrige uma **lacuna do Projeto 02**. A arquitetura inicial previa
"Rate Limiter" e "Circuit Breaker" no worker, sem especificar onde o estado
deles viveria. Ao implementar, ficou claro que a resposta a essa pergunta decide
se o sistema funciona ou não.

O problema, concretamente. O rate limiter precisa responder "posso enviar agora?".
Essa resposta depende de quantas requisições já foram enviadas recentemente. Se
cada worker mantiver essa contagem **na própria memória**:

```
1 worker,  limite 3 req/s  ->  envia 3 req/s    ✓
5 workers, limite 3 req/s  ->  envia 15 req/s   ✗  (3x o limite da plataforma)
```

Cada worker respeitaria o seu limite local perfeitamente. O sistema violaria o
limite da plataforma em 3x. **O bug apareceria exatamente ao escalar** — ou seja,
sob carga, quando o limite mais importa, e nunca em desenvolvimento com um worker.

O mesmo raciocínio vale para o circuit breaker (ADR-006) e para as feature flags
(que precisam propagar a mudança para todas as réplicas).

## Decisão

Adotamos **Redis** como o repositório do estado compartilhado de:

| Estado | Chave | Estrutura |
|---|---|---|
| Token buckets | `apt:rl:platform:<plataforma>` e `apt:rl:content:<hash>` | hash (`tokens`, `ts`) |
| Circuit breaker | `apt:cb:<plataforma>` | hash (`state`, `failures`, `successes`, `opened_at`, `probes`) |
| Feature flags | `apt:flags` | hash (nome → `"0"`/`"1"`) |

## Alternativas consideradas

**Manter o estado no Postgres.** Já está no stack, então não adicionaria serviço.
Recusamos por dois motivos. Desempenho: o rate limiter é consultado **duas vezes
por envio** (eixo do conteúdo + eixo da plataforma), e a essas taxas o custo de
uma transação com WAL por consulta é significativo — o Redis opera em memória.
Concorrência: garantir atomicidade exigiria `SELECT ... FOR UPDATE`, e cada envio
passaria a disputar lock de linha no mesmo registro. Sob 5 workers, o banco viraria
o gargalo do sistema.

**Coordenação entre os workers via mensagens.** Cada worker anunciaria os seus
envios por broadcast e todos manteriam uma visão agregada. Recusado por ser
eventualmente consistente por natureza: existe uma janela entre o envio e o
anúncio chegar aos outros, e nessa janela o limite pode ser violado. Um limite de
vazão precisa de decisão *antes* do envio, não de consenso *depois*.

**Etcd ou ZooKeeper.** Dão consistência forte com consenso (Raft/ZAB), que é mais
garantia do que precisamos. O preço é latência por operação (o consenso exige
quórum) e complexidade operacional muito maior. Redis single-node com scripts
atômicos entrega o que o problema pede.

## Consequências positivas

- **O limite passa a ser GLOBAL.** Escalar workers aumenta a capacidade de
  processamento sem aumentar a vazão enviada. É a tese central da POC e o que o
  `scale_test.py` demonstra.
- **Uma falha vista por qualquer worker abre o circuito para todos** (ADR-006).
  Com estado local, seriam necessárias `threshold × N_workers` falhas.
- **Feature flags propagam para todas as réplicas**, com cache local de 2s +
  invalidação por fanout.
- **Operações de sub-milissegundo.** O rate limiter está no caminho crítico de cada
  envio; qualquer coisa mais lenta seria inviável.
- **Scripts Lua dão atomicidade sem lock distribuído** (ADR-005).

## Consequências negativas

- **Novo ponto único de falha.** Se o Redis cai, o rate limiter e o circuit breaker
  perdem o estado. Mitigamos com **fail-open**: sem Redis, o sistema **permite** o
  envio, registra o evento em nível ERROR e continua protegido pelas camadas
  restantes (bulkhead local, retry com backoff, e o próprio 429 da plataforma).
  A alternativa — fail-closed — transformaria uma queda de Redis em
  indisponibilidade total. A escolha está detalhada em `docs/TRADE-OFFS.md`.
- **O refill depende do relógio dos clientes.** Passamos `now_ms` do worker para o
  script Lua (ADR-005 explica por quê). Relógios defasados entre containers tornam
  o refill levemente impreciso. Protegido por um clamp: o desvio pode deixar o
  limite mais **conservador**, nunca mais permissivo.
- **Mais um serviço no `docker compose`.** Custo pequeno aqui, mas real em produção.
- **Estado em memória.** Habilitamos `appendonly yes` para que os buckets
  sobrevivam a um restart do Redis — não é crítico (o bucket se reconstrói cheio),
  mas evita liberar uma rajada inteira logo após o restart.

## Como validamos

- `tests/integration/test_circuit_breaker_redis.py::TestEstadoCompartilhado::test_falhas_contadas_coletivamente`
  — cinco instâncias distintas de `CircuitBreaker` (simulando cinco processos),
  uma falha cada, e o circuito abre. Com estado local, nenhuma delas chegaria perto
  do threshold.
- `tests/load/scale_test.py` — 1 → 3 → 5 workers com o pico observado pela
  plataforma constante. É a validação de ponta a ponta da decisão.
