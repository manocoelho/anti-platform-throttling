"""Utilitarios compartilhados pelos testes de carga, resiliencia e escala.

Estes testes nao usam pytest. Sao scripts executaveis
(`python -m tests.load.load_test`) porque produzem um RELATORIO, nao um
verde/vermelho: a saida deles vai para `docs/RESULTADOS-TESTES.md`.

O que este modulo concentra:

    espera do stack        `wait_for_stack` -- nao medir antes de tudo estar pronto
    limpeza entre cenarios `reset_all` -- estado residual falseia a medicao
    criacao de campanha    `create_campaign`
    coleta de evidencia    `collect_evidence` -- junta os numeros dos dois lados
    formatacao             tabelas markdown prontas para a documentacao

A COLETA DOS DOIS LADOS E O PONTO CENTRAL

`collect_evidence` le tanto o que NOS registramos (`/admin/outcomes`,
`/admin/latency`, `/admin/throughput`) quanto o que a PLATAFORMA observou
(`/admin/stats` do simulador, com o `peak_rps`).

A evidencia mais forte da POC vem do lado da plataforma: se o pico observado por
ela ficou abaixo do limite dela, o rate limiter cumpriu o objetivo -- medido por
quem imporia a punicao, nao por nos mesmos.
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

API_URL = "http://localhost:8000"
SIM_URL = "http://localhost:9001"


# ---------------------------------------------------------------------------
# Saida no terminal
# ---------------------------------------------------------------------------
def header(title: str) -> None:
    print(f"\n{'=' * 74}\n  {title}\n{'=' * 74}")


def step(message: str) -> None:
    print(f"  -> {message}")


def result(label: str, value: object) -> None:
    print(f"     {label:.<44} {value}")


# ---------------------------------------------------------------------------
# Preparacao do ambiente
# ---------------------------------------------------------------------------
async def wait_for_stack(*, timeout_seconds: float = 90.0) -> bool:
    """Espera API e simulador responderem.

    Medir antes de o stack estar pronto produz numeros que descrevem o boot, nao
    o comportamento do sistema. Especificamente: as primeiras requisicoes
    pagariam o custo de abrir conexao com Postgres e Redis, e apareceriam como
    latencia p99 alta que nao se repete depois.
    """
    step(f"aguardando o stack (timeout {timeout_seconds:.0f}s)...")
    deadline = time.monotonic() + timeout_seconds

    async with httpx.AsyncClient(timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                api = await client.get(f"{API_URL}/health/ready")
                sim = await client.get(f"{SIM_URL}/health")
                if api.status_code == 200 and sim.status_code == 200:
                    step("stack pronto")
                    return True
                if api.status_code == 503:
                    checks = api.json().get("checks", {})
                    faltando = [k for k, v in checks.items() if not v]
                    step(f"API ainda nao pronta; pendente: {faltando}")
            except httpx.HTTPError:
                pass
            await asyncio.sleep(2.0)

    print(
        "\n  ERRO: o stack nao respondeu.\n"
        "  Verifique se o Docker Desktop esta rodando e execute:\n"
        "      docker compose up -d\n"
        "      docker compose ps\n"
    )
    return False


async def reset_all(client: httpx.AsyncClient) -> None:
    """Zera todo o estado mutavel antes de um cenario.

    Sem isso, o resultado de um teste contaminaria o seguinte: um bucket
    parcialmente drenado, um circuito ainda aberto ou o `peak_rps` de um cenario
    anterior mudariam completamente a leitura dos numeros.
    """
    step("zerando estado (buckets, circuitos, flags, contadores do simulador)")
    await client.post(f"{API_URL}/admin/reset/rate-limiter")
    await client.post(f"{API_URL}/admin/reset/circuit-breaker")
    await client.post(f"{API_URL}/flags/reset")
    await client.post(f"{SIM_URL}/admin/reset")
    # Pequena pausa para os workers receberem a invalidacao das flags pelo fanout.
    await asyncio.sleep(0.5)


async def set_flag(client: httpx.AsyncClient, flag: str, value: bool) -> None:
    """Altera uma feature flag e espera a propagacao pelo fanout."""
    await client.patch(f"{API_URL}/flags/{flag}", json={"value": value})
    step(f"flag {flag} = {value}")
    await asyncio.sleep(0.5)


async def inject_fault(
    client: httpx.AsyncClient, platform: str, mode: str, ttl_seconds: int | None = None
) -> None:
    """Injeta uma falha no simulador."""
    payload: dict[str, Any] = {"platform": platform, "mode": mode}
    if ttl_seconds is not None:
        payload["ttl_seconds"] = ttl_seconds
    await client.post(f"{SIM_URL}/admin/fault", json=payload)
    ttl_txt = f", expira em {ttl_seconds}s" if ttl_seconds else ""
    step(f"falha injetada: {platform} -> {mode}{ttl_txt}")


async def clear_fault(client: httpx.AsyncClient, platform: str) -> None:
    await client.delete(f"{SIM_URL}/admin/fault/{platform}")
    step(f"falha removida: {platform}")


# ---------------------------------------------------------------------------
# Campanhas
# ---------------------------------------------------------------------------
async def create_campaign(
    client: httpx.AsyncClient,
    *,
    name: str,
    platform: str,
    total_sends: int,
    target_rate_per_min: float,
    url_count: int = 5,
    jitter_strategy: str = "humanized",
) -> str:
    """Cria e ativa uma campanha. Devolve o id.

    `url_count` importa mais do que parece: o rate limiter tem um eixo POR
    CONTEUDO (4 req/s por URL). Uma campanha com uma URL so teria a vazao
    limitada a 4 req/s independentemente do limite da plataforma -- e o teste
    mediria o eixo errado. Cinco URLs dao folga suficiente para que o gargalo
    seja o limite da plataforma, que e o que queremos observar.
    """
    payload = {
        "name": name,
        "platform": platform,
        "total_sends": total_sends,
        "target_rate_per_min": target_rate_per_min,
        "jitter_strategy": jitter_strategy,
        "contents": [
            {"url": f"https://{platform}.exemplo/conteudo-{i}", "weight": 1}
            for i in range(url_count)
        ],
        "activate": True,
    }
    response = await client.post(f"{API_URL}/campaigns", json=payload)
    response.raise_for_status()
    campaign_id = str(response.json()["id"])
    step(
        f"campanha criada: {name} "
        f"({total_sends} envios, alvo {target_rate_per_min}/min, {url_count} URLs)"
    )
    return campaign_id


async def wait_for_campaign(
    client: httpx.AsyncClient, campaign_id: str, *, timeout_seconds: float = 180.0
) -> dict[str, Any]:
    """Espera a campanha terminar de ser processada.

    "Terminar" aqui significa: nenhuma tarefa em `pending`, `in_flight` ou
    `deferred`. Esperar apenas `status == completed` nao bastaria -- esse status
    indica que o scheduler MATERIALIZOU todas as tarefas, nao que os workers as
    processaram.
    """
    deadline = time.monotonic() + timeout_seconds
    last: dict[str, Any] = {}

    while time.monotonic() < deadline:
        response = await client.get(f"{API_URL}/campaigns/{campaign_id}/status")
        response.raise_for_status()
        last = response.json()

        breakdown = last["task_breakdown"]
        pendentes = (
            breakdown.get("pending", 0)
            + breakdown.get("in_flight", 0)
            + breakdown.get("deferred", 0)
        )
        materializou_tudo = last["campaign"]["status"] in ("completed", "failed")

        if materializou_tudo and pendentes == 0:
            return last

        await asyncio.sleep(2.0)

    step(f"AVISO: timeout de {timeout_seconds:.0f}s aguardando a campanha; segue com o parcial")
    return last


# ---------------------------------------------------------------------------
# Coleta de evidencia
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class Evidence:
    """Numeros de um cenario, dos dois lados da fronteira.

    Attributes:
        outcomes: contagem por resultado, registrada por NOS.
        latency: percentis dos envios aceitos.
        throughput: serie de envios/segundo.
        sim_stats: o que a PLATAFORMA observou -- inclui `peak_rps`, a evidencia
            mais forte da POC.
        workers: distribuicao de envios entre replicas (prova do load balancing).
        breaker_events: transicoes do circuito durante o cenario.
    """

    outcomes: dict[str, int] = field(default_factory=dict)
    latency: dict[str, Any] = field(default_factory=dict)
    throughput: list[dict[str, Any]] = field(default_factory=list)
    sim_stats: list[dict[str, Any]] = field(default_factory=list)
    workers: dict[str, int] = field(default_factory=dict)
    breaker_events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def peak_observed_rps(self) -> dict[str, int]:
        """Pico por segundo que cada plataforma realmente observou."""
        return {s["platform"]: int(s["peak_rps"]) for s in self.sim_stats}

    @property
    def total_throttled_by_platform(self) -> int:
        """Total de 429 devolvidos pelas plataformas.

        E o numero que a POC quer levar a zero. Cada 429 significa que a nossa
        calibragem estava otimista.
        """
        return sum(int(s["total_throttled"]) for s in self.sim_stats)

    @property
    def max_sent_per_second(self) -> int:
        """Maior valor da serie de envios por segundo (medido por nos)."""
        return max((int(p["sent"]) for p in self.throughput), default=0)


async def collect_evidence(client: httpx.AsyncClient, *, platform: str | None = None) -> Evidence:
    """Coleta todos os numeros de um cenario."""
    evidence = Evidence()

    params = {"platform": platform} if platform else {}

    outcomes = await client.get(f"{API_URL}/admin/outcomes", params=params)
    if outcomes.status_code == 200:
        evidence.outcomes = outcomes.json()

    latency = await client.get(f"{API_URL}/admin/latency", params=params)
    if latency.status_code == 200:
        evidence.latency = latency.json()

    if platform:
        throughput = await client.get(
            f"{API_URL}/admin/throughput",
            params={"platform": platform, "window_seconds": 300},
        )
        if throughput.status_code == 200:
            evidence.throughput = throughput.json()

    sim = await client.get(f"{SIM_URL}/admin/stats")
    if sim.status_code == 200:
        evidence.sim_stats = sim.json()

    workers = await client.get(f"{API_URL}/admin/workers")
    if workers.status_code == 200:
        evidence.workers = workers.json()

    events = await client.get(f"{API_URL}/admin/breaker-events")
    if events.status_code == 200:
        evidence.breaker_events = events.json()

    return evidence


def print_evidence(evidence: Evidence, *, platform: str | None = None) -> None:
    """Imprime a evidencia coletada no terminal."""
    print("\n  --- O que NOS registramos ---")
    for outcome, count in sorted(evidence.outcomes.items(), key=lambda kv: -kv[1]):
        result(outcome, count)

    lat = evidence.latency
    if lat.get("samples"):
        print("\n  --- Latencia dos envios aceitos (ms) ---")
        result("amostras", lat["samples"])
        for key in ("p50", "p95", "p99", "avg", "max"):
            if lat.get(key) is not None:
                result(key, f"{lat[key]:.1f}")

    if evidence.throughput:
        result("pico de envios/s (nosso registro)", evidence.max_sent_per_second)

    print("\n  --- O que a PLATAFORMA observou ---")
    for s in evidence.sim_stats:
        if platform and s["platform"] != platform:
            continue
        result(
            f"{s['platform']}: limite {s['limit_rps']}/s | pico observado",
            f"{s['peak_rps']}/s",
        )
        result(f"{s['platform']}: aceitas", s["total_accepted"])
        result(f"{s['platform']}: 429 devolvidos", s["total_throttled"])

    if evidence.workers:
        print("\n  --- Distribuicao entre replicas de worker ---")
        for worker_id, count in sorted(evidence.workers.items(), key=lambda kv: -kv[1]):
            result(worker_id, count)


# ---------------------------------------------------------------------------
# Formatacao para a documentacao
# ---------------------------------------------------------------------------
def markdown_table(headers: list[str], rows: list[list[object]]) -> str:
    """Monta uma tabela markdown pronta para colar na documentacao."""
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
    ]
    lines.extend("| " + " | ".join(str(c) for c in row) + " |" for row in rows)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Controle do Docker (usado pelo teste de escala)
# ---------------------------------------------------------------------------
def scale_workers(count: int) -> bool:
    """Escala as replicas de worker via `docker compose`.

    Devolve False (em vez de estourar) se o comando falhar: o teste de escala
    entao reporta o problema e encerra com mensagem util, em vez de um traceback.
    """
    step(f"escalando para {count} worker(s)...")
    try:
        completed = subprocess.run(
            ["docker", "compose", "up", "-d", "--scale", f"worker={count}", "--no-recreate"],
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        print(f"  ERRO ao escalar workers: {exc}")
        return False

    if completed.returncode != 0:
        print(f"  ERRO ao escalar workers:\n{completed.stderr[:500]}")
        return False

    step(f"{count} worker(s) em execucao")
    return True


def count_running_workers() -> int:
    """Quantas replicas de worker estao no ar (via `docker compose ps`)."""
    try:
        completed = subprocess.run(
            ["docker", "compose", "ps", "-q", "worker"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return 0
    return len([line for line in completed.stdout.splitlines() if line.strip()])
