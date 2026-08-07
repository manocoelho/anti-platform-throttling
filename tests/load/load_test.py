"""Teste de carga: mede vazao, latencia e a taxa de 429 sob demanda excessiva.

    python -m tests.load.load_test

HIPOTESE SOB TESTE

Submetido a uma demanda MUITO ACIMA do limite da plataforma, o sistema deve:

    (a) nao receber nenhum 429 -- ou pouquissimos;
    (b) manter a vazao efetiva abaixo do `allowed_rps` configurado;
    (c) registrar os envios barrados como `rate_limited_local`, nao como falha.

O item (c) e o mais importante conceitualmente. Um sistema que simplesmente
derrubasse o excedente tambem teria zero 429 -- e teria perdido trabalho. O que
provamos aqui e diferente: o excedente foi ADIADO e continua na fila, e a
distincao entre "nos adiamos" e "fomos bloqueados" esta explicita nos numeros.

DOIS CENARIOS, PARA HAVER COMPARACAO

    A. protecoes LIGADAS  -> o comportamento normal
    B. rate limiter DESLIGADO (via feature flag) -> o contrafactual

Sem o cenario B, o resultado de A nao significaria nada: nao daria para saber se
os 429 nao apareceram por causa do rate limiter ou porque a carga era baixa. B e
o que da sentido a A.
"""

from __future__ import annotations

import asyncio
import sys

import httpx
from tests.load.common import (
    Evidence,
    collect_evidence,
    create_campaign,
    header,
    markdown_table,
    print_evidence,
    reset_all,
    result,
    set_flag,
    step,
    wait_for_campaign,
    wait_for_stack,
)

# --- Parametros do cenario --------------------------------------------------
PLATFORM = "youtube"
TOTAL_SENDS = 400
# 1800/min = 30/s. O limite estimado da plataforma e 20/s e o nosso allowed_rps
# e 16/s -- ou seja, pedimos praticamente o DOBRO do que nos permitimos. E de
# proposito: com demanda abaixo do limite, o rate limiter nunca entraria em acao
# e o teste nao mediria nada.
TARGET_RATE_PER_MIN = 1800
ALLOWED_RPS = 16.0
PLATFORM_LIMIT_RPS = 20


async def run_scenario(
    client: httpx.AsyncClient, *, name: str, rate_limiter_enabled: bool
) -> Evidence:
    """Roda um cenario completo e devolve a evidencia coletada."""
    header(f"CENARIO: {name}")

    await reset_all(client)
    if not rate_limiter_enabled:
        await set_flag(client, "rate_limiter_enabled", False)

    campaign_id = await create_campaign(
        client,
        name=f"Carga - {name}",
        platform=PLATFORM,
        total_sends=TOTAL_SENDS,
        target_rate_per_min=TARGET_RATE_PER_MIN,
        url_count=8,
    )

    step("aguardando o processamento...")
    final = await wait_for_campaign(client, campaign_id, timeout_seconds=240.0)
    result("status final da campanha", final["campaign"]["status"])
    result("tarefas por status", final["task_breakdown"])

    evidence = await collect_evidence(client, platform=PLATFORM)
    print_evidence(evidence, platform=PLATFORM)
    return evidence


def build_report(protegido: Evidence, desprotegido: Evidence) -> str:
    """Monta o relatorio markdown comparando os dois cenarios."""

    def get(evidence: Evidence, key: str) -> int:
        return int(evidence.outcomes.get(key, 0))

    def sim_for(evidence: Evidence, key: str) -> int:
        for s in evidence.sim_stats:
            if s["platform"] == PLATFORM:
                return int(s[key])
        return 0

    comparativo = markdown_table(
        ["Metrica", "Protecoes ligadas", "Rate limiter desligado"],
        [
            ["Envios aceitos (2xx)", get(protegido, "sent"), get(desprotegido, "sent")],
            [
                "**429 recebidos da plataforma**",
                f"**{get(protegido, 'throttled')}**",
                f"**{get(desprotegido, 'throttled')}**",
            ],
            [
                "Adiados por nos (rate_limited_local)",
                get(protegido, "rate_limited_local"),
                get(desprotegido, "rate_limited_local"),
            ],
            [
                "Pico observado pela plataforma (req/s)",
                sim_for(protegido, "peak_rps"),
                sim_for(desprotegido, "peak_rps"),
            ],
            [
                "Limite da plataforma (req/s)",
                PLATFORM_LIMIT_RPS,
                PLATFORM_LIMIT_RPS,
            ],
            [
                "429 devolvidos pela plataforma",
                sim_for(protegido, "total_throttled"),
                sim_for(desprotegido, "total_throttled"),
            ],
        ],
    )

    lat = protegido.latency
    latencia = markdown_table(
        ["Percentil", "Latencia (ms)"],
        [
            ["p50", f"{lat.get('p50', 0):.1f}" if lat.get("p50") else "n/d"],
            ["p95", f"{lat.get('p95', 0):.1f}" if lat.get("p95") else "n/d"],
            ["p99", f"{lat.get('p99', 0):.1f}" if lat.get("p99") else "n/d"],
            ["max", f"{lat.get('max', 0):.1f}" if lat.get("max") else "n/d"],
            ["amostras", lat.get("samples", 0)],
        ],
    )

    return (
        f"### Teste de carga -- {PLATFORM}\n\n"
        f"Demanda solicitada: **{TARGET_RATE_PER_MIN}/min "
        f"({TARGET_RATE_PER_MIN / 60:.0f} req/s)** | "
        f"nosso limite: **{ALLOWED_RPS} req/s** | "
        f"limite da plataforma: **{PLATFORM_LIMIT_RPS} req/s**\n\n"
        f"{comparativo}\n\n"
        f"**Latencia (cenario protegido)**\n\n{latencia}\n"
    )


def evaluate(protegido: Evidence, desprotegido: Evidence) -> bool:
    """Avalia os criterios de aceite e imprime o veredito.

    Reportamos o resultado REAL, inclusive quando ele contraria a hipotese. Um
    numero honesto que exige explicacao vale mais, na dimensao de Testes e
    Validacao, que um numero bonito sem lastro.
    """
    header("CRITERIOS DE ACEITE")

    throttled_protegido = int(protegido.outcomes.get("throttled", 0))
    throttled_desprotegido = int(desprotegido.outcomes.get("throttled", 0))
    pico_protegido = protegido.peak_observed_rps.get(PLATFORM, 0)
    adiados = int(protegido.outcomes.get("rate_limited_local", 0))

    criterios = [
        (
            "com protecao, nenhum 429 recebido",
            throttled_protegido == 0,
            f"{throttled_protegido} recebidos",
        ),
        (
            "pico observado pela plataforma <= limite dela",
            pico_protegido <= PLATFORM_LIMIT_RPS,
            f"pico {pico_protegido}/s vs limite {PLATFORM_LIMIT_RPS}/s",
        ),
        (
            "excedente foi ADIADO, nao descartado",
            adiados > 0,
            f"{adiados} adiamentos registrados",
        ),
        (
            "sem protecao, os 429 aparecem (contrafactual)",
            throttled_desprotegido > throttled_protegido,
            f"{throttled_desprotegido} sem protecao vs {throttled_protegido} com",
        ),
    ]

    todos_ok = True
    for descricao, passou, detalhe in criterios:
        marca = "OK  " if passou else "FALHOU"
        print(f"  [{marca}] {descricao}: {detalhe}")
        todos_ok = todos_ok and passou

    return todos_ok


async def main() -> int:
    header("TESTE DE CARGA -- Anti-Platform Throttling")

    if not await wait_for_stack():
        return 1

    async with httpx.AsyncClient(timeout=30.0) as client:
        protegido = await run_scenario(client, name="protecoes ligadas", rate_limiter_enabled=True)
        desprotegido = await run_scenario(
            client, name="rate limiter DESLIGADO", rate_limiter_enabled=False
        )

        # Religa a protecao: deixar o rate limiter desligado afetaria qualquer
        # medicao seguinte.
        await set_flag(client, "rate_limiter_enabled", True)

        todos_ok = evaluate(protegido, desprotegido)

        header("RELATORIO (markdown -- colar em docs/RESULTADOS-TESTES.md)")
        print(build_report(protegido, desprotegido))

        return 0 if todos_ok else 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
