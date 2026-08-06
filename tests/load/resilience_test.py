"""Teste de resiliencia: circuit breaker abrindo, recuperando, e o bulkhead isolando.

    python -m tests.load.resilience_test

DUAS HIPOTESES SOB TESTE

H1 -- CIRCUIT BREAKER
     Quando o Instagram passa a devolver erro 500, o circuito daquela plataforma
     deve ABRIR (paramos de tentar), e depois que a falha se resolve deve SONDAR
     e FECHAR sozinho. A evidencia sao as linhas em `breaker_events`.

H2 -- BULKHEAD
     Enquanto o Instagram esta fora, o YouTube deve continuar enviando na vazao
     normal. Sem isolamento, os envios lentos ou falhos do Instagram consumiriam
     os slots de execucao compartilhados e a vazao do YouTube cairia junto --
     falha em cascata.

H2 e a hipotese mais interessante, porque e a que um sistema sem bulkhead
falharia de forma silenciosa: nao apareceria erro nenhum, apenas a vazao do
YouTube despencando por um motivo que nao tem nada a ver com o YouTube.

DESENHO DO EXPERIMENTO

    t=0    duas campanhas ativas (YouTube e Instagram), ambas saudaveis
    t=~8s  injeta error_500 no Instagram, com TTL de 25s
    ...    o circuito do Instagram abre; o YouTube segue
    t=~35s a falha expira sozinha; o circuito sonda e fecha
    fim    compara a vazao do YouTube durante e depois da falha

A falha tem TTL para que a recuperacao aconteca sem intervencao externa no meio
da medicao -- o momento exato de uma chamada manual influenciaria o resultado.
"""

from __future__ import annotations

import asyncio
import sys
import time

import httpx
from tests.load.common import (
    API_URL,
    SIM_URL,
    collect_evidence,
    create_campaign,
    header,
    inject_fault,
    markdown_table,
    print_evidence,
    reset_all,
    step,
    wait_for_stack,
)

FAULT_PLATFORM = "instagram"
HEALTHY_PLATFORM = "youtube"

# Campanhas longas o suficiente para atravessar todo o experimento sem esgotar o
# orcamento no meio -- uma campanha que termina antes da recuperacao deixaria de
# gerar trafego e nao daria para observar o circuito fechando.
TOTAL_SENDS = 900
TARGET_RATE_PER_MIN = 600  # 10/s por campanha

FAULT_DELAY_SECONDS = 8.0
FAULT_TTL_SECONDS = 25
OBSERVATION_SECONDS = 60.0


async def sample_state(client: httpx.AsyncClient) -> dict[str, object]:
    """Tira uma amostra do estado das duas plataformas."""
    platforms = await client.get(f"{API_URL}/platforms")
    sim = await client.get(f"{SIM_URL}/admin/stats")

    circuits = (
        {p["platform"]: p.get("circuit_state") for p in platforms.json()}
        if platforms.status_code == 200
        else {}
    )
    accepted = (
        {s["platform"]: int(s["total_accepted"]) for s in sim.json()}
        if sim.status_code == 200
        else {}
    )

    return {"circuits": circuits, "accepted": accepted}


async def main() -> int:
    header("TESTE DE RESILIENCIA -- circuit breaker e bulkhead")

    if not await wait_for_stack():
        return 1

    async with httpx.AsyncClient(timeout=30.0) as client:
        await reset_all(client)

        # --- Prepara as duas campanhas ---------------------------------------
        step("criando uma campanha para cada plataforma")
        await create_campaign(
            client,
            name="Resiliencia - YouTube (saudavel)",
            platform=HEALTHY_PLATFORM,
            total_sends=TOTAL_SENDS,
            target_rate_per_min=TARGET_RATE_PER_MIN,
            url_count=6,
        )
        await create_campaign(
            client,
            name="Resiliencia - Instagram (vai falhar)",
            platform=FAULT_PLATFORM,
            total_sends=TOTAL_SENDS,
            target_rate_per_min=TARGET_RATE_PER_MIN,
            url_count=6,
        )

        timeline: list[dict[str, object]] = []
        started = time.monotonic()
        fault_injected = False
        # Marcos de aceitas do YouTube: antes, durante e depois da falha.
        yt_before = yt_during = yt_after = 0

        header("OBSERVACAO")
        step(
            f"observando por {OBSERVATION_SECONDS:.0f}s; "
            f"a falha entra em t={FAULT_DELAY_SECONDS:.0f}s e dura {FAULT_TTL_SECONDS}s"
        )

        while True:
            elapsed = time.monotonic() - started
            if elapsed > OBSERVATION_SECONDS:
                break

            # Injeta a falha no momento planejado.
            if not fault_injected and elapsed >= FAULT_DELAY_SECONDS:
                sample = await sample_state(client)
                yt_before = int(sample["accepted"].get(HEALTHY_PLATFORM, 0))  # type: ignore[union-attr]
                await inject_fault(
                    client, FAULT_PLATFORM, "error_500", ttl_seconds=FAULT_TTL_SECONDS
                )
                fault_injected = True

            sample = await sample_state(client)
            circuits = sample["circuits"]  # type: ignore[assignment]
            accepted = sample["accepted"]  # type: ignore[assignment]

            timeline.append(
                {
                    "t": round(elapsed, 1),
                    "circuito_instagram": circuits.get(FAULT_PLATFORM),  # type: ignore[union-attr]
                    "circuito_youtube": circuits.get(HEALTHY_PLATFORM),  # type: ignore[union-attr]
                    "aceitas_youtube": accepted.get(HEALTHY_PLATFORM, 0),  # type: ignore[union-attr]
                    "aceitas_instagram": accepted.get(FAULT_PLATFORM, 0),  # type: ignore[union-attr]
                }
            )

            # Fim da janela de falha: marca as aceitas do YouTube.
            if (
                fault_injected
                and yt_during == 0
                and elapsed >= (FAULT_DELAY_SECONDS + FAULT_TTL_SECONDS)
            ):
                yt_during = int(accepted.get(HEALTHY_PLATFORM, 0))  # type: ignore[union-attr]

            print(
                f"     t={elapsed:5.1f}s | "
                f"circuito IG={circuits.get(FAULT_PLATFORM)!s:9s} | "  # type: ignore[union-attr]
                f"circuito YT={circuits.get(HEALTHY_PLATFORM)!s:9s} | "  # type: ignore[union-attr]
                f"aceitas YT={accepted.get(HEALTHY_PLATFORM, 0):4d} | "  # type: ignore[union-attr]
                f"aceitas IG={accepted.get(FAULT_PLATFORM, 0):4d}"  # type: ignore[union-attr]
            )
            await asyncio.sleep(3.0)

        final = await sample_state(client)
        yt_after = int(final["accepted"].get(HEALTHY_PLATFORM, 0))  # type: ignore[union-attr]

        # --- Evidencia ------------------------------------------------------
        header("EVIDENCIA COLETADA")
        evidence = await collect_evidence(client)
        print_evidence(evidence)

        estados_ig = [t["circuito_instagram"] for t in timeline if t["circuito_instagram"]]
        estados_yt = [t["circuito_youtube"] for t in timeline if t["circuito_youtube"]]

        print("\n  --- Transicoes registradas em breaker_events ---")
        if evidence.breaker_events:
            for event in reversed(evidence.breaker_events[:12]):
                print(
                    f"     {event['platform']:10s} "
                    f"{event['from_state']:9s} -> {event['to_state']:9s} "
                    f"({event.get('reason') or 'sem motivo registrado'})"
                )
        else:
            print("     nenhuma transicao registrada")

        # --- Criterios de aceite --------------------------------------------
        header("CRITERIOS DE ACEITE")

        ig_abriu = "open" in estados_ig
        ig_sondou = "half_open" in estados_ig
        ig_fechou_depois = bool(estados_ig) and estados_ig[-1] == "closed"
        yt_nunca_abriu = "open" not in estados_yt

        # H2: o YouTube manteve vazao durante a falha do Instagram?
        yt_durante = max(0, (yt_during or yt_after) - yt_before)
        yt_isolado = yt_durante > 0

        criterios = [
            (
                "H1a: o circuito do Instagram ABRIU",
                ig_abriu,
                f"estados observados: {sorted(set(estados_ig))}",
            ),
            (
                "H1b: o circuito SONDOU a recuperacao (half_open)",
                ig_sondou,
                "half_open observado" if ig_sondou else "half_open nao capturado",
            ),
            (
                "H1c: o circuito FECHOU apos a falha expirar",
                ig_fechou_depois,
                f"estado final: {estados_ig[-1] if estados_ig else 'n/d'}",
            ),
            (
                "H2a: o circuito do YouTube NUNCA abriu",
                yt_nunca_abriu,
                f"estados observados: {sorted(set(estados_yt))}",
            ),
            (
                "H2b: o YouTube continuou enviando durante a falha",
                yt_isolado,
                f"{yt_durante} envios aceitos durante a janela de falha",
            ),
        ]

        todos_ok = True
        for descricao, passou, detalhe in criterios:
            marca = "OK  " if passou else "FALHOU"
            print(f"  [{marca}] {descricao}: {detalhe}")
            todos_ok = todos_ok and passou

        if not ig_sondou:
            print(
                "\n  NOTA: o estado half_open e transitorio -- dura apenas o tempo das\n"
                "  sondas. Amostrando a cada 3s, e possivel que ele tenha existido e\n"
                "  nao tenha sido capturado. As linhas de breaker_events acima sao a\n"
                "  evidencia definitiva: elas registram TODAS as transicoes."
            )

        # --- Relatorio ------------------------------------------------------
        header("RELATORIO (markdown -- colar em docs/RESULTADOS-TESTES.md)")

        amostras = markdown_table(
            ["t (s)", "Circuito Instagram", "Circuito YouTube", "Aceitas YT", "Aceitas IG"],
            [
                [
                    t["t"],
                    t["circuito_instagram"],
                    t["circuito_youtube"],
                    t["aceitas_youtube"],
                    t["aceitas_instagram"],
                ]
                # Uma amostra a cada 2 para a tabela nao ficar longa demais.
                for t in timeline[::2]
            ],
        )

        transicoes = (
            markdown_table(
                ["Plataforma", "De", "Para", "Motivo"],
                [
                    [e["platform"], e["from_state"], e["to_state"], e.get("reason") or "-"]
                    for e in reversed(evidence.breaker_events[:12])
                ],
            )
            if evidence.breaker_events
            else "_Nenhuma transicao registrada._"
        )

        print(
            f"### Teste de resiliencia\n\n"
            f"Falha injetada: `error_500` no **{FAULT_PLATFORM}** em "
            f"t={FAULT_DELAY_SECONDS:.0f}s, com TTL de {FAULT_TTL_SECONDS}s "
            f"(auto-expira).\n\n"
            f"**Linha do tempo**\n\n{amostras}\n\n"
            f"**Transicoes do circuit breaker**\n\n{transicoes}\n\n"
            f"**Isolamento (bulkhead):** o YouTube aceitou **{yt_durante}** envios "
            f"durante a janela em que o Instagram estava fora do ar.\n"
        )

        return 0 if todos_ok else 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
