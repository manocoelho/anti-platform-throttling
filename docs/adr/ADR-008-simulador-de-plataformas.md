# ADR-008 — Simulador próprio em vez de mocks ou APIs reais

**Status:** Aceita
**Origem:** Projeto 03 (o Projeto 02 mencionava "APIs simuladas" sem detalhar)

## Contexto

A POC precisa provar que o sistema **evita** receber 429. Provar isso exige que
exista um lado capaz de **devolver** 429 sob as condições reais que o provocam.

Três caminhos possíveis, e cada um responde a uma pergunta diferente.

## Decisão

Construímos um **simulador próprio** (`src/apt/platform_sim/`) que:

- aplica limite de vazão por **janela deslizante** real, com contagem exata;
- devolve **429 com header `Retry-After`** ao exceder;
- expõe injeção de falhas (`error_500`, `timeout`, `throttle_hard`), com TTL para
  auto-expiração;
- reporta o **`peak_rps` que ele observou** — a evidência mais forte do projeto.

## Alternativas consideradas

### Mocks em teste unitário

Um `AsyncMock` configurado para devolver 429 quando mandamos.

Recusado porque **não reproduz o fenômeno**. Throttling é um comportamento
**emergente**: depende de quantas requisições chegam, com que espaçamento, dentro
de qual janela de contagem. Um mock que devolve 429 sob comando prova apenas que o
nosso código **trata** 429 — não que o nosso rate limiter **evita** receber 429.

A diferença entre as duas afirmações é o projeto inteiro.

### APIs reais de YouTube, Instagram e TikTok

Seria a validação definitiva. **Não é uma opção**, e vale dizer por quê
explicitamente:

1. **Uso abusivo de serviço de terceiros.** Enviar volume de tráfego artificial a
   APIs alheias com o objetivo de descobrir onde elas começam a bloquear é, na
   prática, um teste de carga não autorizado contra a infraestrutura de outra
   pessoa.
2. **Violação de termos de uso.** Os termos dessas plataformas proíbem
   explicitamente engajamento automatizado e tentativas de contornar limites.
3. **Não seria reproduzível.** Os limites reais são dinâmicos, variam por conta,
   por região e por histórico. Duas execuções do mesmo teste dariam resultados
   diferentes, e nenhum deles seria verificável por quem avalia o trabalho.
4. **Exigiria contas reais**, com risco de banimento e implicações que vão além do
   técnico.

O que a POC demonstra é o **mecanismo** de respeitar um limite desconhecido com
margem de segurança. Não é, e não pretende ser, a descoberta dos limites de
nenhuma plataforma específica.

### Um serviço de terceiros que simule rate limiting (`httpbin`, WireMock)

`httpbin.org/status/429` devolve 429, e o WireMock permite cenários com estado.
Recusado por dar menos controle do que precisamos: não conseguiríamos ler o
`peak_rps` observado, nem injetar timeout com TTL, nem garantir que o algoritmo do
outro lado é **diferente** do nosso — que é o ponto seguinte.

## A decisão que torna o teste honesto: algoritmos diferentes nas duas pontas

| | Algoritmo |
|---|---|
| Nosso rate limiter | **token bucket** |
| O simulador | **janela deslizante** |

A assimetria é **deliberada**, e é o que separa este teste de uma tautologia.

Se as duas pontas usassem o mesmo algoritmo com os mesmos parâmetros, o nosso
limiter acertaria o limite **por construção**. O teste provaria apenas que
`3 < 5` — aritmética, não engenharia.

Com algoritmos diferentes, as janelas de contagem **não se alinham**. Uma rajada
permitida pelo nosso bucket (que tolera `capacity` requisições instantâneas) pode
cair inteira dentro da janela do simulador e estourar o limite dele. É
precisamente por isso que a **margem de segurança** existe — 20% para o
Instagram, 40% para o YouTube (que soma um segundo motivo à margem maior: ver
TRADE-OFFS.md item 19) — e o teste é o que verifica se ela é suficiente.

Nota adicional: janela deslizante com log de timestamps foi *rejeitada* para o
nosso limiter por causa do custo de memória (ADR-004). Aqui ela é a escolha certa:
precisão exata é desejável num instrumento de medição, e o volume é controlado.

## Sobre os números: eles são estimativas, e isso está no código

Os limites atribuídos às plataformas são **estimativas escolhidas para um ambiente
controlado de laboratório**:

| Plataforma | Limite atribuído | Nosso `allowed_rps` | Margem |
|---|---|---|---|
| YouTube | 5 req/s | 3 req/s | 40% |
| Instagram | 10 req/s | 8 req/s | 20% |

Não são números oficiais — as plataformas não os publicam, e eles mudam sem aviso.
Os valores foram calibrados para três objetivos didáticos: limites **assimétricos**,
para que o bulkhead tenha plataformas de perfis diferentes para isolar; limites
**baixos**, para que um teste de carga complete em segundos; e, no caso do YouTube,
um limite abaixo do teto de vazão agregada do ambiente de teste (uma VM
compartilhada, ~6-8 req/s), para que o rate limiter — e não o hardware do
ambiente — seja o gargalo observado (ver TRADE-OFFS.md item 19 e
RESULTADOS-TESTES.md § 1.6). A margem do YouTube (40%) é maior que a do
Instagram (20%) por esse segundo motivo, não porque a estimativa do limite em
si seja menos confiável.

Essa ressalva está registrada no docstring de
`src/apt/domain/platforms.py`, na descrição da API (`/docs`) e no schema de
resposta de `GET /platforms` — não apenas neste documento. Um número apresentado
como oficial quando é estimativa é uma afirmação falsa, mesmo quando o mecanismo em
volta está correto.

## Consequências positivas

- **O fenômeno é reproduzível.** O mesmo teste dá o mesmo resultado, e qualquer
  pessoa pode executá-lo com `docker compose up`.
- **A evidência vem do lado de quem imporia a punição.** `peak_rps` é o pico que a
  *plataforma* observou. Se ficou abaixo do limite dela durante todo o teste, o
  rate limiter cumpriu o objetivo — medido por ela, não por nós.
- **Injeção de falha com TTL viabiliza o teste de resiliência.** Observamos o
  circuito abrir **e** fechar numa única execução, sem intervenção manual no meio da
  medição (o momento exato de uma chamada manual influenciaria o resultado).
- **Nenhuma dependência externa.** O stack sobe e roda offline.
- **Zero implicação ética ou legal.**

## Consequências negativas

- **Não prova nada sobre as plataformas reais.** É a limitação fundamental, e está
  declarada. A POC valida o mecanismo, não os números.
- **Um serviço a mais para manter.** ~350 linhas, incluindo os endpoints
  administrativos.
- **O simulador precisa estar correto.** Se ele tiver bug, todos os resultados de
  carga estão errados. Por isso `tests/unit/test_platform_sim.py` testa a janela
  deslizante e o `peak_rps` com o relógio controlado por monkeypatch.
- **A simulação é simplificada.** Plataformas reais têm limites por endpoint, por
  token, por região, penalidades progressivas e detecção comportamental. O
  simulador cobre limite de vazão e falha, e não pretende mais que isso.

## Como validamos

- `tests/unit/test_platform_sim.py::TestJanelaDeslizante` — verifica a expiração da
  janela e, principalmente, que `peak_rps` registra o máximo histórico e **não
  diminui** quando a janela esvazia.
- `tests/unit/test_platform_sim.py::TestFaultConfig::test_falha_com_ttl_expira` —
  garante a auto-expiração de que o teste de resiliência depende.
- `tests/load/load_test.py` — usa o `peak_rps` do simulador como critério de aceite
  principal, e compara os cenários com e sem rate limiter.
