# ADR-005 — Script Lua para garantir atomicidade

**Status:** Aceita
**Origem:** Projeto 03

> **Este é o ADR mais importante do projeto.** Ele registra a decisão que faz o
> rate limiter funcionar de verdade num sistema distribuído. Sem ela, o sistema
> teria um rate limiter que passa em qualquer teste com um worker e falha
> exatamente ao escalar.

## Contexto

Decidido que o estado fica no Redis (ADR-003) e que o algoritmo é token bucket
(ADR-004), resta a pergunta: **como consultar e atualizar esse estado sem
condição de corrida?**

A implementação natural tem três passos:

```python
tokens = await redis.hget(key, "tokens")   # (1) LÊ
if tokens >= 1:                            # (2) DECIDE
    await redis.hset(key, "tokens", tokens - 1)   # (3) ESCREVE
    await enviar()
```

Entre (1) e (3) existe uma janela. Cinco workers podem ler `tokens = 1`
simultaneamente, todos concluírem que podem enviar, e **cinco requisições saírem
quando havia orçamento para uma**.

Duas propriedades tornam esse bug especialmente perigoso:

1. **Ele só se manifesta sob concorrência** — ou seja, sob carga, quando o limite
   mais importa. Em desenvolvimento, com um worker, o código parece correto.
2. **Escalar piora o problema.** Mais workers, mais concorrência, mais estouro. O
   sistema violaria o limite justamente quando estivesse fazendo o seu trabalho.

O mesmo raciocínio vale para o circuit breaker: dois workers leem
`failure_count = 4`, ambos incrementam, ambos gravam 5 — e uma falha se perde na
contagem. Ou, em `half_open`, três workers leem `probes = 1` com limite 2, todos
concluem que há vaga, e as três sondas viram uma rajada sobre um serviço que
acabou de se recuperar.

## Decisão

A lógica de decisão roda **dentro do Redis**, em scripts Lua:

| Script | O que faz |
|---|---|
| `src/apt/resilience/lua/token_bucket.lua` | refill + decisão + escrita do bucket |
| `src/apt/resilience/lua/circuit_breaker.lua` | transições de estado do circuito |

O Redis executa cada script de forma **atômica**: enquanto ele roda, nenhum outro
comando é processado. Ler, calcular, decidir e gravar tornam-se uma operação
indivisível. **A janela deixa de existir.**

Os scripts são carregados via `SCRIPT LOAD` e invocados por `EVALSHA <hash>` — só
o hash de 40 caracteres trafega a cada chamada, não o corpo do script.

## Alternativas consideradas

**`INCR` / `DECR` (operações atômicas nativas).** São atômicas, e seria a solução
mais simples. Recusadas por não bastarem para o algoritmo escolhido: `DECR` não
sabe fazer refill baseado em tempo, não sabe limitar o saldo à capacidade máxima, e
não sabe calcular quanto falta para a próxima ficha. Daria um contador por janela
fixa — que sofre do efeito de borda rejeitado no ADR-004.

**`WATCH` + `MULTI`/`EXEC` (transação otimista).** É a forma idiomática de
read-modify-write no Redis sem scripts. Funciona: `WATCH` faz o `EXEC` falhar se a
chave mudou, e o cliente tenta de novo. Recusada porque sob **alta contenção** — que
é o nosso caso, todos os workers disputando a mesma chave — a taxa de retry
dispara. Com 5 workers concorrentes na mesma chave, boa parte das transações
falharia e seria repetida, gastando round trips. O script Lua resolve em uma
única ida à rede, sempre.

**Lock distribuído (Redlock ou `SET NX` com TTL).** Adquirir lock, ler, decidir,
escrever, liberar. Recusado por dois motivos. Primeiro, custo: são no mínimo duas
operações extras (adquirir e liberar) por envio, no caminho crítico. Segundo, e
mais grave: se o worker morre entre adquirir e liberar, o lock fica preso até o TTL
expirar — e **todos** os workers ficam bloqueados nesse intervalo. Introduziríamos
um modo de falha novo para resolver um problema que o Lua resolve sem lock nenhum.

**Fazer a decisão num serviço centralizado próprio.** Um microsserviço "rate
limiter" com o estado em memória, consultado por todos os workers. Recusado: seria
reimplementar o Redis com pior desempenho, e o serviço viraria um SPOF sem
persistência nem replicação.

## Detalhes de implementação que merecem registro

**`now_ms` vem do cliente, não do `redis.call('TIME')`.** Duas razões:
*testabilidade* — o teste de paridade injeta o mesmo timestamp na implementação Lua
e na de referência em Python e compara os resultados, o que seria impossível com o
relógio interno do servidor; e *histórico de replicação* — scripts que leem o
relógio eram considerados não determinísticos em versões antigas do Redis. O custo
é depender do relógio dos workers, mitigado pelo clamp de `elapsed` (ADR-004).

**As fichas voltam multiplicadas por 1000.** O protocolo do Redis trunca números
de retorno para inteiro; devolver `0.85` fichas chegaria como `0`. O script devolve
`850` e o cliente divide — preserva três casas decimais.

**A biblioteca trata `NOSCRIPT` sozinha.** Se o Redis reiniciar e perder o cache de
scripts, o `redis-py` recebe o erro, reenvia o corpo e repete a chamada
automaticamente.

## Consequências positivas

- **A condição de corrida é eliminada por construção**, não por convenção. Não
  depende de nenhum programador lembrar de usar lock.
- **Uma ida à rede por decisão.** Metade do custo da versão read-modify-write, e
  sem retry por contenção.
- **O comportamento não muda com o número de workers.** É o que o `scale_test.py`
  demonstra e a base da tese da POC.
- **`half_open` respeita a cota de sondas de verdade.** Sem atomicidade, três
  workers passariam simultaneamente por um limite de 2.

## Consequências negativas

- **O algoritmo existe duas vezes: em Lua e em Python.** É o custo mais real desta
  decisão. Aceitamos deliberadamente, porque a versão Python é o que torna possível
  testar exaustivamente os casos de borda sem infraestrutura, e é a *explicação
  legível* do algoritmo. O risco (corrigir uma e esquecer a outra) é coberto por um
  **teste de paridade** que roda a mesma sequência nas duas implementações e compara
  os resultados. Registrado em `docs/TRADE-OFFS.md`.
- **Lua é uma linguagem a mais no projeto.** Ninguém da equipe a conhecia. Mitigado
  mantendo os scripts curtos (~90 linhas cada, com comentário abundante) e sem
  nenhuma lógica que não esteja também na versão Python.
- **Debugar dentro do Redis é ruim.** Não há debugger nem stack trace útil; erro de
  script vira `ResponseError` genérico. Mitigado com validação explícita no início
  de cada script (`redis.error_reply` com mensagem descritiva) e com o teste de
  paridade, que localiza a divergência no passo exato.
- **Script longo bloquearia o Redis.** Como a execução é atômica, um script lento
  travaria o servidor inteiro. Os nossos fazem O(1) operações — mas é uma
  restrição a respeitar em qualquer alteração futura.

## Como validamos

- **`tests/integration/test_rate_limiter_redis.py::TestConcorrencia::test_concorrencia_nao_estoura_o_limite`**
  — 50 corrotinas simultâneas disputando um bucket de capacidade 10, com timestamp
  fixo para congelar o refill. Exatamente 10 passam. **Com read-modify-write, este
  teste falharia.** É a validação direta desta decisão.
- **`TestParidade::test_paridade_com_a_implementacao_de_referencia`** — 30 passos
  de 100ms, cobrindo esgotamento e refill parcial, comparando Lua × Python em
  `allowed`, `tokens_remaining` e `retry_after_ms`. É a rede que protege contra a
  duplicação do algoritmo.
- **`tests/integration/test_circuit_breaker_redis.py::TestConcorrencia::test_concorrencia_nao_perde_contagem`**
  — falhas simultâneas de N workers, todas contadas.
