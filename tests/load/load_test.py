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

POR QUE O SCRIPT ESCALA PARA 5 WORKERS ANTES DE COMECAR

Com `prefetch=1`, um UNICO worker processa mensagens essencialmente em serie --
a proxima so e entregue apos o ack da anterior, que so acontece apos o ciclo
completo (breaker + rate limiter + envio HTTP + registro). Isso poe um teto
natural de ~15-20 req/s na vazao que um worker sozinho consegue GERAR,
independente de qualquer flag. Numa medicao real, esse teto ficou ainda mais
baixo (~7-9 req/s) por contencao da VM.

O efeito pratico: com 1 worker, `rate_limiter_enabled=False` nao muda nada
observavel, porque a demanda real nunca chegou perto do limite de qualquer
plataforma -- o contrafactual mede o teto do consumidor, nao o do rate
limiter. Com 5 workers a demanda agregada supera o teto de 1 worker sozinho,
mas isso sozinho nao bastava: enquanto o YouTube estava calibrado em 16 req/s
permitidos (limite estimado 20), o teto de vazao AGREGADA desta VM
(~6-8 req/s, mesmo com 5 workers) ficava ABAIXO do que o rate limiter chegaria
a restringir -- o contrafactual media o teto do ambiente, nao o do mecanismo,
e por isso `rate_limiter_enabled=False` continuava sem produzir 429 mesmo com
5 workers (ver RESULTADOS-TESTES.md secao 1.6, tres hipoteses eliminadas por
medicao).

POR ISSO O YOUTUBE FOI RECALIBRADO PARA 3/1/5 (allowed/burst/estimado)

Um teto de ambiente medido em ~6-8 req/s so vira um confundidor quando o
`allowed_rps` do mecanismo fica PROXIMO ou ACIMA dele -- e 16 req/s estava.
Recalibrando o YouTube para 3 req/s permitidos (bem abaixo do teto de
~6-8 req/s do ambiente), o rate limiter volta a restringir antes que o
hardware entre em jogo: com a protecao ligada, a vazao fica em ~3 req/s;
desligada, a demanda (>=10 req/s, ver TARGET_RATE_PER_MIN) sobe ate o teto do
ambiente (~6-8 req/s), que agora excede o limite do simulador (5 req/s) o
suficiente para 429 aparecerem de verdade. O Instagram nao mudou -- ver
src/apt/domain/platforms.py e TRADE-OFFS.md.
"""

from __future__ import annotations

import asyncio
import sys

import httpx
from tests.load.common import (
    Evidence,
    collect_evidence,
    count_running_workers,
    create_campaign,
    header,
    markdown_table,
    print_evidence,
    reset_all,
    result,
    scale_workers,
    set_flag,
    step,
    wait_for_campaign,
    wait_for_stack,
)

# --- Parametros do cenario --------------------------------------------------
PLATFORM = "youtube"
# TOTAL_SENDS reduzido de 400 para 150 nesta recalibracao. Motivo diferente do
# de scale_test.py: aqui o cenario "rate limiter DESLIGADO" agora produz 429
# de verdade (era o objetivo), e cada 429 aciona um retry com backoff (TTL
# 1s/5s/30s por tentativa, ate 4 tentativas) -- com 400 envios e demanda
# acima do limite do simulador, uma fracao grande cai em retry e o tempo
# total explode (medido: nao terminou em 6min40s). Com 150, o mesmo efeito
# (429 aparecem, protecao os evita) fica visivel numa duracao administravel.
TOTAL_SENDS = 150
# 600/min = 10/s de demanda: acima do allowed_rps do mecanismo (3/s) e acima
# do teto de vazao agregada da VM de teste (~6-8/s), o suficiente para o rate
# limiter ser o gargalo com a flag ligada e para o teto do ambiente (que
# agora excede o limite do simulador, 5/s) produzir 429 de verdade com a flag
# desligada -- ver a nota "POR ISSO O YOUTUBE FOI RECALIBRADO" acima.
TARGET_RATE_PER_MIN = 600
ALLOWED_RPS = 3.0
PLATFORM_LIMIT_RPS = 5
SCALE_WORKERS = 5


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
        # "uniform", nao o "humanized" padrao de create_campaign(): a
        # estrategia HUMANIZED multiplica a demanda pelo perfil de atividade
        # da HORA ATUAL (hour_utc) -- entre 0h-5h UTC, esse multiplicador cai
        # para 0.12-0.25, suprimindo a demanda real para bem menos que
        # TARGET_RATE_PER_MIN independentemente de qualquer flag. Um teste de
        # carga controlado nao pode ter seu resultado dependente da hora do
        # relogio -- foi isso, e nao a calibracao, que fez a demanda cair a
        # ~2 req/s numa execucao de madrugada. Ver RESULTADOS-TESTES.md.
        jitter_strategy="uniform",
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

    step(f"escalando para {SCALE_WORKERS} worker(s) -- ver nota no docstring do modulo...")
    if not scale_workers(SCALE_WORKERS):
        print("\n  ERRO: nao foi possivel escalar. Docker Compose disponivel?\n")
        return 1
    # Os workers precisam conectar ao broker e declarar a topologia antes de
    # comecar a consumir -- mesma espera que scale_test.py usa.
    await asyncio.sleep(12.0)
    running = count_running_workers()
    result("replicas em execucao", running)
    if running != SCALE_WORKERS:
        step(f"AVISO: esperado {SCALE_WORKERS} replica(s), encontrado {running}")

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            protegido = await run_scenario(
                client, name="protecoes ligadas", rate_limiter_enabled=True
            )
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
    finally:
        # Volta a configuracao padrao para nao deixar 5 replicas rodando.
        step("restaurando 1 worker...")
        scale_workers(1)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
