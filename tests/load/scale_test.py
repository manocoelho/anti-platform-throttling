"""Teste de escala: a prova de que o rate limiter e realmente DISTRIBUIDO.

    python -m tests.load.scale_test

ESTE E O TESTE MAIS IMPORTANTE DO PROJETO.

HIPOTESE

Escalar de 1 para 3 para 5 workers deve aumentar a CAPACIDADE DE
PROCESSAMENTO do sistema, mas NAO a vazao enviada a plataforma. O pico de
requisicoes que a plataforma observa deve permanecer abaixo do `allowed_rps`
configurado nas tres configuracoes.

POR QUE ISSO PROVA ALGO

Um rate limiter em memoria de processo passaria em qualquer teste com 1 worker.
Com 5 workers, cada um teria o seu proprio balde de 16 req/s e o sistema enviaria
80 req/s -- 4x o limite da plataforma.

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
ALLOWED_RPS = 16.0
PLATFORM_LIMIT_RPS = 20

# Demanda bem acima do limite, para que o rate limiter seja o gargalo em TODAS as
# configuracoes. Com demanda baixa, 1 e 5 workers dariam o mesmo resultado e o
# teste nao provaria nada.
TOTAL_SENDS = 500
TARGET_RATE_PER_MIN = 2400  # 40/s solicitados vs 16/s permitidos

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

    # --- Criterio 3: A HIPOTESE CENTRAL ------------------------------------
    # O pico nao deve crescer proporcionalmente ao numero de workers. Toleramos
    # 25% de variacao entre a menor e a maior configuracao -- o jitter e o
    # desalinhamento entre a nossa janela e a do simulador produzem alguma
    # oscilacao legitima. Crescimento LINEAR (5x com 5 workers) seria a
    # assinatura inequivoca de um limiter por processo.
    picos = [s.peak_observed_rps for s in scenarios]
    if len(picos) >= 2 and min(picos) > 0:
        crescimento = max(picos) / min(picos)
        passou = crescimento <= 1.25
        marca = "OK  " if passou else "FALHOU"
        print(
            f"  [{marca}] o pico NAO cresce com o numero de workers: "
            f"variacao de {crescimento:.2f}x entre {min(picos)}/s e {max(picos)}/s "
            f"(tolerancia 1.25x; um limiter por processo daria ~{len(picos)}x)"
        )
        todos_ok = todos_ok and passou

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

    picos = [s.peak_observed_rps for s in scenarios]
    variacao = f"{max(picos) / min(picos):.2f}x" if picos and min(picos) > 0 else "n/d"

    return (
        f"### Teste de escala -- a prova do rate limiter distribuido\n\n"
        f"Demanda solicitada: **{TARGET_RATE_PER_MIN}/min "
        f"({TARGET_RATE_PER_MIN / 60:.0f} req/s)** | "
        f"nosso limite: **{ALLOWED_RPS} req/s** | "
        f"limite da plataforma: **{PLATFORM_LIMIT_RPS} req/s**\n\n"
        f"{tabela}\n\n"
        f"**Variacao do pico entre a menor e a maior configuracao: {variacao}**\n\n"
        f"Com um rate limiter em memoria de processo, o pico cresceria "
        f"proporcionalmente ao numero de workers (~{len(scenarios)}x). O estado "
        f"compartilhado no Redis e o que mantem o limite GLOBAL.\n\n"
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
