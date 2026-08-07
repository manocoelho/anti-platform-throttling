"""Teste de escala: a prova de que o rate limiter e realmente DISTRIBUIDO.

    python -m tests.load.scale_test

ESTE E O TESTE MAIS IMPORTANTE DO PROJETO.

HIPOTESE

Escalar de 1 para 3 para 5 workers deve aumentar a CAPACIDADE DE
PROCESSAMENTO do sistema, mas NAO a vazao enviada a plataforma. O pico de
requisicoes que a plataforma observa deve permanecer dentro do TETO ALGEBRICO
do bucket compartilhado (`allowed_rps + burst_capacity`) nas tres
configuracoes -- nao so abaixo de `allowed_rps` isoladamente, porque o pior
caso (balde cheio + refill do mesmo segundo) pode legitimamente somar as duas
parcelas numa unica janela de 1s do simulador.

POR QUE ISSO PROVA ALGO

Um rate limiter em memoria de processo passaria em qualquer teste com 1 worker.
Com 5 workers, cada um teria o seu proprio balde de 3 req/s e o sistema enviaria
15 req/s -- 3x o limite da plataforma.

Ou seja: o rate limiter existiria, estaria "funcionando" em cada processo, e o
sistema violaria o limite exatamente ao escalar. O bug so apareceria em producao,
sob carga, no pior momento possivel.

A unica forma de escalar sem esse problema e o estado do limite ser
COMPARTILHADO. E o que este teste verifica de ponta a ponta.

O QUE MAIS O TESTE MOSTRA

A distribuicao de envios entre as replicas (`/admin/workers`) comprova o padrao
Load Balancing: com `prefetch=1`, os competing consumers do RabbitMQ distribuem
a carga de forma aproximadamente uniforme. Uma replica com 90% dos envios
indicaria prefetch alto demais.

PRE-REQUISITO

O Docker precisa estar rodando, porque o teste executa
`docker compose up -d --scale worker=N` entre os cenarios.
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass

import httpx
from tests.load.common import (
    Evidence,
    collect_evidence,
    count_running_workers,
    create_campaign,
    header,
    markdown_table,
    reset_all,
    result,
    scale_workers,
    step,
    wait_for_campaign,
    wait_for_stack,
)

PLATFORM = "youtube"
ALLOWED_RPS = 3.0
PLATFORM_LIMIT_RPS = 5
# Espelha o burst_capacity do YouTube em src/apt/domain/platforms.py. Usado pelo
# criterio C-3 abaixo -- ver o comentario em evaluate() para o porque.
BURST_CAPACITY = 1
# Teto algebrico do bucket: no pior caso (balde cheio + refill do mesmo
# segundo), uma unica janela de 1s do simulador pode ver ate esta quantidade de
# requisicoes -- e a propria invariante que test_domain.py::
# test_burst_mais_refill_nao_passa_do_limite_estimado verifica. Ver TRADE-OFFS.md
# item 16 e item 20.
ALGEBRAIC_CEILING_RPS = ALLOWED_RPS + BURST_CAPACITY

# Demanda bem acima do limite, para que o rate limiter seja o gargalo em TODAS as
# configuracoes. Com demanda baixa, 1 e 5 workers dariam o mesmo resultado e o
# teste nao provaria nada.
#
# TOTAL_SENDS reduzido de 500 para 150 nesta recalibracao: com o YouTube em
# 3 req/s permitidos (antes 16), o mesmo volume por cenario levaria ~5.3x mais
# tempo para escoar (a vazao de envio passa a ser limitada pelo mecanismo, nao
# mais pelo teto do ambiente). Sem reduzir o volume, os tres cenarios juntos
# ultrapassariam os ~10 minutos aceitaveis para a demo ao vivo. Ver
# PLANO-DE-TESTES.md e RESULTADOS-TESTES.md para a duracao medida.
TOTAL_SENDS = 150
TARGET_RATE_PER_MIN = 600  # 10/s solicitados vs 3/s permitidos

WORKER_COUNTS = (1, 3, 5)


@dataclass(slots=True)
class ScenarioResult:
    """Resultado de um cenario com N workers."""

    workers: int
    sent: int
    throttled: int
    deferred: int
    peak_observed_rps: int
    max_sent_per_second: int
    p95_latency_ms: float | None
    worker_distribution: dict[str, int]
    duration_seconds: float

    @property
    def respected_limit(self) -> bool:
        """Se o pico observado pela plataforma ficou dentro do limite dela."""
        return self.peak_observed_rps <= PLATFORM_LIMIT_RPS

    @property
    def distribution_ratio(self) -> float:
        """Razao entre a replica mais e a menos usada.

        1.0 = distribuicao perfeita. Valores muito altos indicam que uma replica
        absorveu quase tudo -- sintoma de prefetch alto demais.
        """
        counts = [c for c in self.worker_distribution.values() if c > 0]
        if len(counts) < 2:
            return 1.0
        return max(counts) / min(counts)


async def run_scenario(client: httpx.AsyncClient, worker_count: int) -> ScenarioResult | None:
    """Escala os workers, roda a carga e coleta o resultado."""
    header(f"CENARIO: {worker_count} worker(s)")

    if not scale_workers(worker_count):
        return None

    # Os workers precisam conectar ao broker e declarar a topologia antes de
    # comecar a consumir. Medir durante o boot deles atribuiria a vazao baixa do
    # startup ao rate limiter.
    step("aguardando os workers conectarem...")
    await asyncio.sleep(12.0)

    running = count_running_workers()
    result("replicas em execucao", running)
    if running != worker_count:
        step(f"AVISO: esperado {worker_count} replica(s), encontrado {running}")

    await reset_all(client)

    loop = asyncio.get_running_loop()
    started = loop.time()

    campaign_id = await create_campaign(
        client,
        name=f"Escala - {worker_count} workers",
        platform=PLATFORM,
        total_sends=TOTAL_SENDS,
        target_rate_per_min=TARGET_RATE_PER_MIN,
        url_count=10,
        # "uniform", nao "humanized": ver a nota em load_test.py sobre o
        # multiplicador de atividade por hora do dia suprimir a demanda real
        # de madrugada (UTC), independentemente do rate limiter.
        jitter_strategy="uniform",
    )

    step("aguardando o processamento...")
    await wait_for_campaign(client, campaign_id, timeout_seconds=300.0)
    duration = loop.time() - started

    evidence: Evidence = await collect_evidence(client, platform=PLATFORM)

    scenario = ScenarioResult(
        workers=running or worker_count,
        sent=int(evidence.outcomes.get("sent", 0)),
        throttled=int(evidence.outcomes.get("throttled", 0)),
        deferred=int(evidence.outcomes.get("rate_limited_local", 0)),
        peak_observed_rps=evidence.peak_observed_rps.get(PLATFORM, 0),
        max_sent_per_second=evidence.max_sent_per_second,
        p95_latency_ms=evidence.latency.get("p95"),
        worker_distribution=evidence.workers,
        duration_seconds=duration,
    )

    result("envios aceitos", scenario.sent)
    result("429 recebidos", scenario.throttled)
    result("adiados pelo rate limiter", scenario.deferred)
    result(
        "PICO observado pela plataforma",
        f"{scenario.peak_observed_rps}/s (limite dela: {PLATFORM_LIMIT_RPS}/s)",
    )
    result("pico no nosso registro", f"{scenario.max_sent_per_second}/s")
    result("duracao", f"{duration:.1f}s")
    if scenario.p95_latency_ms:
        result("latencia p95", f"{scenario.p95_latency_ms:.1f}ms")

    print("\n     distribuicao entre replicas:")
    for worker_id, count in sorted(scenario.worker_distribution.items(), key=lambda kv: -kv[1]):
        result(f"  {worker_id}", count)

    return scenario


def evaluate(scenarios: list[ScenarioResult]) -> bool:
    """Avalia a hipotese central e imprime o veredito."""
    header("CRITERIOS DE ACEITE")

    if not scenarios:
        print("  [FALHOU] nenhum cenario executou com sucesso")
        return False

    todos_ok = True

    # --- Criterio 1: o limite foi respeitado em TODAS as configuracoes -------
    for s in scenarios:
        passou = s.respected_limit
        marca = "OK  " if passou else "FALHOU"
        print(
            f"  [{marca}] com {s.workers} worker(s), o pico observado pela plataforma "
            f"({s.peak_observed_rps}/s) <= limite dela ({PLATFORM_LIMIT_RPS}/s)"
        )
        todos_ok = todos_ok and passou

    # --- Criterio 2: nenhum 429 em nenhuma configuracao ---------------------
    for s in scenarios:
        passou = s.throttled == 0
        marca = "OK  " if passou else "FALHOU"
        print(f"  [{marca}] com {s.workers} worker(s), 429 recebidos = {s.throttled}")
        todos_ok = todos_ok and passou

    # --- Criterio 3: A HIPOTESE CENTRAL -------------------------------------
    # A hipotese testada e "o limite e global, entao escalar workers NAO
    # AUMENTA o pico alem do que o proprio bucket compartilhado permite".
    #
    # Ate a rodada anterior, isso era medido por uma razao relativa contra a
    # linha de base (peak_rps(N) <= peak_rps(1) x 1.15). Essa razao CONTRADIZ
    # a invariante que o proprio projeto declara em test_domain.py::
    # test_burst_mais_refill_nao_passa_do_limite_estimado: burst_capacity +
    # allowed_rps <= estimated_limit_rps. Com o YouTube recalibrado
    # (allowed=3, burst=1), a especificacao permite 1+3=4 req/s numa unica
    # janela do simulador -- e um teto relativo de 1.15x sobre uma linha de
    # base de 3 e 3.45, que PROIBE o 4 que a propria invariante do projeto
    # autoriza. O criterio relativo nao escala para um baseline pequeno: o
    # menor incremento inteiro possivel (uma unica requisicao) ja e 33% de um
    # baseline de 3, mas era so 6% de um baseline de 16 (o numero para o qual
    # 1.15x foi originalmente calibrado). Errado o criterio, nao a medicao --
    # ver TRADE-OFFS.md item 20.
    #
    # Substituido por medicao direta contra os dois tetos que a hipotese
    # central realmente afirma:
    #   (a) o pico nunca excede o teto ALGEBRICO do bucket compartilhado
    #       (allowed_rps + burst_capacity) em NENHUMA configuracao -- e esse
    #       teto NAO cresce com o numero de workers, porque o estado e
    #       compartilhado;
    #   (b) o pico nunca excede o limite da propria plataforma (ja verificado
    #       no Criterio 1 acima, com o mesmo numero quando estimated_limit_rps
    #       == PLATFORM_LIMIT_RPS).
    # Um rate limiter em memoria de processo violaria (a) exatamente ao
    # escalar (cada processo teria seu proprio balde) -- e essa violacao,
    # nao uma razao percentual, e a assinatura que este criterio detecta.
    for s in scenarios:
        passou = s.peak_observed_rps <= ALGEBRAIC_CEILING_RPS
        marca = "OK  " if passou else "FALHOU"
        print(
            f"  [{marca}] com {s.workers} worker(s), o pico ({s.peak_observed_rps}/s) <= teto "
            f"algebrico do bucket compartilhado ({ALGEBRAIC_CEILING_RPS:g}/s = "
            f"allowed {ALLOWED_RPS:g} + burst {BURST_CAPACITY}); um limiter por processo "
            f"violaria este teto exatamente ao escalar"
        )
        todos_ok = todos_ok and passou

    # A razao relativa contra a linha de base so tem significado quando o
    # incremento inteiro minimo (1 req/s) e pequeno frente ao baseline --
    # arbitrado aqui em baseline >= 20, o mesmo numero para o qual a
    # tolerancia de 1.15x foi originalmente calibrada. Informativo apenas:
    # nao participa de todos_ok.
    if len(scenarios) >= 2 and scenarios[0].peak_observed_rps >= 20:
        baseline = scenarios[0]
        for s in scenarios[1:]:
            crescimento = s.peak_observed_rps / baseline.peak_observed_rps
            passou = crescimento <= 1.15
            marca = "OK  " if passou else "FALHOU"
            print(
                f"  [{marca}] com {s.workers} worker(s), o pico ({s.peak_observed_rps}/s) nao "
                f"cresce mais que 15% sobre a linha de base de {baseline.workers} worker(s) "
                f"({baseline.peak_observed_rps}/s): {crescimento:.2f}x "
                f"(tolerancia 1.15x; um limiter por processo daria ~{s.workers}x)"
            )
            todos_ok = todos_ok and passou
    elif len(scenarios) >= 2:
        print(
            f"  [INFO] criterio de crescimento relativo (1.15x) nao aplicavel: linha de "
            f"base de {scenarios[0].peak_observed_rps}/s < 20/s -- quantizacao inteira "
            f"torna a razao sem significado (ver PLANO-DE-TESTES.md e TRADE-OFFS.md item 20)"
        )

    # --- Criterio 4: load balancing entre replicas --------------------------
    for s in scenarios:
        if s.workers < 2:
            continue
        ativas = len([c for c in s.worker_distribution.values() if c > 0])
        passou = ativas >= 2 and s.distribution_ratio <= 4.0
        marca = "OK  " if passou else "FALHOU"
        print(
            f"  [{marca}] com {s.workers} worker(s), {ativas} replica(s) receberam "
            f"trabalho; razao maior/menor = {s.distribution_ratio:.2f} (tolerancia 4.0)"
        )
        todos_ok = todos_ok and passou

    return todos_ok


def build_report(scenarios: list[ScenarioResult]) -> str:
    """Monta o relatorio markdown."""
    tabela = markdown_table(
        [
            "Workers",
            "Aceitos",
            "**429**",
            "Adiados",
            "**Pico observado pela plataforma**",
            "Duracao (s)",
            "p95 (ms)",
        ],
        [
            [
                s.workers,
                s.sent,
                f"**{s.throttled}**",
                s.deferred,
                f"**{s.peak_observed_rps}/s**",
                f"{s.duration_seconds:.1f}",
                f"{s.p95_latency_ms:.1f}" if s.p95_latency_ms else "n/d",
            ]
            for s in scenarios
        ],
    )

    distribuicoes = "\n\n".join(
        f"**{s.workers} worker(s)** -- distribuicao de envios entre as replicas:\n\n"
        + markdown_table(
            ["Replica", "Envios"],
            [
                [wid, count]
                for wid, count in sorted(s.worker_distribution.items(), key=lambda kv: -kv[1])
            ],
        )
        for s in scenarios
        if s.workers >= 2 and s.worker_distribution
    )

    maior_pico = max((s.peak_observed_rps for s in scenarios), default=0)
    dentro_do_teto = maior_pico <= ALGEBRAIC_CEILING_RPS

    return (
        f"### Teste de escala -- a prova do rate limiter distribuido\n\n"
        f"Demanda solicitada: **{TARGET_RATE_PER_MIN}/min "
        f"({TARGET_RATE_PER_MIN / 60:.0f} req/s)** | "
        f"nosso limite: **{ALLOWED_RPS} req/s** | "
        f"limite da plataforma: **{PLATFORM_LIMIT_RPS} req/s**\n\n"
        f"{tabela}\n\n"
        f"**Teto algebrico do bucket compartilhado (allowed + burst): "
        f"{ALGEBRAIC_CEILING_RPS:g}/s -- maior pico observado em qualquer configuracao: "
        f"{maior_pico}/s ({'dentro do teto' if dentro_do_teto else 'EXCEDEU O TETO'})**\n\n"
        f"Com um rate limiter em memoria de processo, o pico cresceria "
        f"proporcionalmente ao numero de workers (~{len(scenarios)}x o teto acima) -- "
        f"muito alem do teto algebrico de um unico balde compartilhado. O estado "
        f"compartilhado no Redis e o que mantem o pico dentro desse teto em qualquer "
        f"numero de workers.\n\n"
        f"{distribuicoes}\n"
    )


async def main() -> int:
    header("TESTE DE ESCALA -- Anti-Platform Throttling")
    print(
        "  Hipotese: escalar workers aumenta a capacidade de PROCESSAMENTO,\n"
        "  mas nao a vazao ENVIADA a plataforma.\n"
    )

    if not await wait_for_stack():
        return 1

    scenarios: list[ScenarioResult] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for count in WORKER_COUNTS:
            scenario = await run_scenario(client, count)
            if scenario is not None:
                scenarios.append(scenario)

        todos_ok = evaluate(scenarios)

        header("RELATORIO (markdown -- colar em docs/RESULTADOS-TESTES.md)")
        print(build_report(scenarios))

    # Volta a configuracao padrao para nao deixar 5 replicas rodando.
    step("restaurando 1 worker...")
    scale_workers(1)

    return 0 if todos_ok else 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
