# ADR-004 — Token bucket em vez de janela deslizante

**Status:** Aceita
**Origem:** Projeto 03

## Contexto

O Projeto 02 definiu que haveria um "Rate Limiter", sem escolher o algoritmo. A
escolha importa, porque os algoritmos disponíveis têm perfis de falha diferentes —
e um deles tem um modo de falha que é exatamente o que a POC precisa evitar.

Os quatro candidatos clássicos:

| Algoritmo | Estado por chave | Permite rajada? | Problema |
|---|---|---|---|
| Janela fixa | 1 contador | sim, na borda | **efeito de borda** (ver abaixo) |
| Janela deslizante (log) | 1 timestamp **por requisição** | não | memória cresce com o volume |
| Token bucket | 2 números | sim, controlada | precisa de float |
| Leaky bucket | 2 números + fila | não | vazão perfeitamente constante |

## Decisão

Adotamos **token bucket**, com dois parâmetros por eixo de limitação:

- `refill_rps` — vazão sustentada (fichas reposta por segundo)
- `burst_capacity` — tamanho máximo da rajada tolerada

O estado é apenas `(tokens: float, updated_at_ms: int)`. Não há temporizador
repondo fichas: o refill é **calculado na leitura**.

```
tokens_agora = min(capacity, tokens + (agora - updated_at) * refill_rps)
```

Implementação de referência em `src/apt/resilience/token_bucket.py` (pura,
testável) e execução atômica em `src/apt/resilience/lua/token_bucket.lua`
(ADR-005).

## Alternativas consideradas

**Janela fixa (contador por segundo).** A implementação mais simples possível:
`INCR` numa chave que expira em 1s. Recusada pelo **efeito de borda**. Com limite
de 20 req/s, um cliente pode enviar 20 requisições em `12:00:00.999` e outras 20
em `12:00:01.001` — 40 requisições em 2 milissegundos, dentro do limite formal em
cada janela. Para a plataforma que está do outro lado, isso é uma rajada de 40
req/s. É precisamente o comportamento que dispara throttling, e o algoritmo o
permitiria por construção.

**Janela deslizante com log de timestamps.** Precisão exata: guarda o timestamp de
cada requisição e conta quantos caem na janela. Recusada pelo custo de memória, que
é **proporcional ao volume**: a 20 req/s são 20 timestamps por chave; a 20.000
req/s, 20.000. A estrutura cresce justamente sob carga, quando menos se quer isso.
Some-se que temos uma chave por URL de conteúdo — uma campanha com 500 URLs
multiplicaria o problema.

*Curiosidade que virou decisão de projeto:* usamos esse algoritmo **no simulador**
(`src/apt/platform_sim/throttle.py`), de propósito. Precisão exata é desejável
num instrumento de medição, o volume ali é controlado, e ter algoritmos
**diferentes** nas duas pontas é o que torna o teste honesto (ADR-008).

**Janela deslizante aproximada (média ponderada de duas janelas).** É o meio-termo
usado por muitos CDNs: memória constante, sem efeito de borda grave. Recusada por
não permitir rajada controlada e por ser mais difícil de explicar — o token bucket
resolve o mesmo problema com um modelo mental que cabe numa frase.

**Leaky bucket.** Impõe vazão perfeitamente constante, sem rajada. Recusado por
uma razão específica desta POC: vazão perfeitamente regular é **assinatura de
máquina**. Os mecanismos de detecção olham a forma da distribuição, não só o
volume. Queremos o oposto de regularidade perfeita — daí o jitter
(`src/apt/scheduling/jitter.py`).

## Consequências positivas

- **Estado constante: dois números por chave.** Independente do volume. Importa
  porque esse estado vive no Redis e é lido a cada envio.
- **Rajada controlada e explícita.** Um bucket cheio absorve `capacity`
  requisições instantâneas e depois converge para a vazão sustentada. O tamanho da
  rajada é um parâmetro, não um acidente.
- **Sem efeito de borda.** O refill é contínuo no tempo, não discreto por janela.
- **Devolve `retry_after_ms`.** O algoritmo sabe calcular quando haverá ficha
  suficiente — é o que permite ao worker escolher o degrau de retry correto em vez
  de adivinhar (ADR-009).
- **Custo O(1) sem estrutura auxiliar.** Uma leitura, um cálculo, uma escrita.

## Consequências negativas

- **Exige aritmética de ponto flutuante.** Com inteiros, uma vazão de 0.5 req/s
  truncaria para zero a cada leitura e o bucket nunca encheria. Consequência: o
  script Lua precisa devolver as fichas multiplicadas por 1000, porque o protocolo
  do Redis trunca números de retorno (detalhe documentado no cabeçalho do `.lua`).
- **Sensível ao relógio.** O refill depende de `agora - updated_at`. Relógios
  defasados entre workers tornam o cálculo impreciso. Protegido por
  `max(0, elapsed)`: o desvio pode deixar o limite mais conservador, nunca mais
  permissivo. Sem esse clamp, um `elapsed` negativo **removeria** fichas — um bug
  intermitente e praticamente impossível de diagnosticar.
- **Bucket cheio libera rajada após inatividade.** Um bucket parado enche até a
  capacidade; o primeiro envio depois disso pode sair em rajada de `capacity`
  requisições. Mitigado por manter `burst_capacity + allowed_rps <=
  estimated_limit_rps` da plataforma — invariante verificada em
  `tests/unit/test_domain.py::test_burst_mais_refill_nao_passa_do_limite_estimado`.
  Uma versão anterior desta invariante checava só `burst_capacity <=
  estimated_limit_rps`, ignorando o refill do mesmo segundo — insuficiente, e a
  causa raiz dos 429 reais medidos na primeira execução dos testes de carga
  (ver TRADE-OFFS.md, item 16).
- **Dois parâmetros para calibrar em vez de um.** `refill_rps` e `burst_capacity`
  precisam ser escolhidos juntos.

## Como validamos

- `tests/unit/test_token_bucket.py` — 17 casos, incluindo bucket vazio, refill
  fracionário, relógio para trás, pedido maior que a capacidade e a garantia de que
  **negar não consome crédito**.
- `test_rajada_esgota_bucket_e_depois_converge_para_a_vazao` — verifica a
  propriedade central: 20 requisições instantâneas contra capacidade 16 → exatamente
  16 passam.
- `test_vazao_sustentada_respeita_o_limite_configurado` — 1000 tentativas ao longo
  de 10s (demanda de 100 req/s contra limite de 16 req/s) e o total aceito fica
  dentro de `capacity + rps × 10`.
