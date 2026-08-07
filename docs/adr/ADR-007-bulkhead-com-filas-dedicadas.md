# ADR-007 — Bulkhead com filas e pools dedicados por plataforma

**Status:** Aceita
**Origem:** Projeto 03 (padrão não previsto no Projeto 02)

## Contexto

O nome vem da engenharia naval. Um navio é dividido em compartimentos estanques
(*bulkheads*): se um inunda, os outros seguem secos e o navio flutua. Sem eles, uma
perfuração em qualquer ponto afunda o casco inteiro.

O cenário concreto no nosso sistema, **sem** bulkhead:

O Instagram começa a responder em 5 segundos em vez de 20ms. Cada envio para o
Instagram ocupa uma corrotina do worker por 5 segundos. Como os envios de YouTube e
Instagram compartilham o mesmo pool de execução, em poucos segundos **todos** os
slots estão presos esperando o Instagram — e os envios de YouTube, que responderiam
normalmente, ficam na fila atrás deles.

O resultado é o pior tipo de falha: **silenciosa**. Nenhum erro aparece. A vazão do
YouTube simplesmente despenca, por um motivo que não tem nada a ver com o YouTube.
Quem investiga vai olhar o YouTube primeiro, e não vai encontrar nada.

## Decisão

Isolamos os recursos por plataforma em **três camadas**, cada uma protegendo um
recurso diferente:

| Camada | Recurso isolado | Onde vive |
|---|---|---|
| **Fila dedicada** | posição na fila (o broker) | `messaging/topology.py` — `apt.tasks.<plataforma>` |
| **Semáforo** | slots de execução (o worker) | `resilience/bulkhead.py` — `asyncio.Semaphore` |
| **Pool HTTP** | conexões de rede | `worker/sender.py` — um `AsyncClient` por plataforma |

As três são necessárias. Cada uma sozinha deixa um recurso compartilhado:

- só fila dedicada → as corrotinas ainda compartilham os slots de execução;
- só semáforo → mil tarefas de Instagram acumuladas ainda ficam à frente das de
  YouTube na fila única;
- só pool HTTP → nem a posição na fila nem os slots estão isolados.

As cotas são **assimétricas** (YouTube 8 slots, Instagram 4), acompanhando a
diferença de limite entre as plataformas.

## Comportamento em caso de saturação: fail-fast, não espera

`Bulkhead.acquire()` tem timeout (2s por padrão). Esgotado o prazo sem vaga, o envio
é **recusado** e a tarefa volta para a fila de retry — não espera.

Isso é deliberado. Uma espera sem limite transformaria o semáforo numa **fila
invisível**: as tarefas não apareceriam em lugar nenhum (nem na fila do RabbitMQ, nem
em execução), o consumo de memória cresceria em silêncio, e a latência medida
perderia significado — mediria o tempo na fila invisível, não o tempo de serviço.

Recusar rápido e devolver a tarefa à fila mantém o estado do sistema **visível**.

## Alternativas consideradas

**Fila única com um pool de execução compartilhado.** O desenho mais simples e o
ponto de partida natural. Descartado pelo cenário do Contexto: é exatamente a
configuração que produz falha em cascata silenciosa.

**Um processo de worker por plataforma.** Isolamento em nível de sistema
operacional — o mais forte possível. Uma degradação do Instagram não teria como
tocar o processo do YouTube. Recusado por dois motivos práticos: o número de
containers passaria a crescer com o número de plataformas (2 plataformas × 5
réplicas = 10 containers), e a alocação ficaria rígida — 5 workers de Instagram
ficariam ociosos enquanto o YouTube tem fila, porque não podem ajudar. O semáforo dá
isolamento suficiente com alocação elástica.

**Thread pool separado por plataforma.** Descartado por incompatibilidade com o
modelo do projeto: o sistema é assíncrono de ponta a ponta (ADR-002), e o recurso
escasso é o slot de corrotina, não a thread. Threads adicionariam troca de contexto
sem resolver nada.

**Timeout agressivo em vez de bulkhead.** Se cada envio tivesse timeout de 500ms, a
degradação do Instagram seria contida por si só. Recusado porque trata o sintoma:
com 4 slots ocupados por 500ms cada, ainda haveria contenção, e um timeout curto
demais transformaria latência alta legítima em falha. O timeout existe no projeto (5s),
mas como último recurso — não como mecanismo de isolamento.

## Consequências positivas

- **Uma plataforma degradada não afeta a outra.** É a propriedade central, e é
  demonstrável ao vivo: injetar falha no Instagram e ver o YouTube seguir.
- **A saturação fica visível.** `bulkhead.rejected_total` cresce, e o gauge
  `apt_bulkhead_in_use` mostra a ocupação por plataforma. Um número que sobe indica
  ou cota pequena demais ou plataforma lenta — nos dois casos, o compartimento está
  fazendo o seu trabalho e contendo o problema.
- **As cotas são explícitas e ajustáveis** por plataforma, via `.env`.
- **Filas dedicadas dão bônus operacional.** No painel do RabbitMQ dá para ver a
  profundidade da fila de cada plataforma separadamente — durante a apresentação, é
  a visualização mais direta do isolamento.
- **`max_in_use` indica dimensionamento.** Se o pico ficar sempre bem abaixo da
  capacidade, a cota pode ser reduzida.

## Consequências negativas

- **Capacidade fragmentada.** Os 4 slots do Instagram ficam ociosos quando só o
  YouTube tem trabalho. Um pool compartilhado teria utilização maior. É o
  trade-off clássico do padrão: trocamos eficiência de utilização por isolamento de
  falha. Aceitamos porque o custo da falha em cascata é muito maior que o de alguns
  slots parados.
- **Mais parâmetros para calibrar.** Uma cota por plataforma, mais o timeout de
  aquisição.
- **O semáforo é local ao worker.** Cada réplica tem os seus 8 slots de YouTube,
  então 5 réplicas dão 40 slots totais. Isso é **intencional**: o bulkhead limita
  recursos *deste processo* (corrotinas, conexões), e esses recursos são locais. O
  limite que precisa de visão global é o de vazão, e esse sim é distribuído
  (ADR-003).
- **Risco de vazar slot.** Um `release()` esquecido reduziria a capacidade
  permanentemente, e após N vazamentos a plataforma pararia de ser atendida por
  aquele worker. Mitigado com `try/finally` obrigatório no worker e por teste
  específico.

## Como validamos

- **`tests/unit/test_bulkhead.py::TestIsolamento::test_plataforma_esgotada_nao_afeta_a_outra`**
  — esgota o Instagram por completo e verifica que os 4 slots do YouTube seguem
  livres. É a validação direta da propriedade central.
- **`test_timeout_nao_vaza_slot`** — após 5 timeouts consecutivos, a capacidade
  original continua disponível. Guarda o modo de falha mais perigoso do padrão.
- `test_carga_concorrente_respeita_a_capacidade` — 20 corrotinas concorrentes, no
  máximo 3 dentro do compartimento simultaneamente.
- `tests/integration/test_messaging.py::TestTopologia::test_uma_fila_por_plataforma`
  — confirma o isolamento na camada do broker.
- `tests/load/resilience_test.py` (hipótese **H2**) — mede quantos envios o YouTube
  aceitou **durante** a janela em que o Instagram estava fora do ar. É a validação
  de ponta a ponta.
