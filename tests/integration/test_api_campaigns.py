"""Testes de integracao da API: CRUD de campanhas contra o Postgres real.

Usamos `httpx.ASGITransport` para falar com a aplicacao em processo, sem subir o
uvicorn. A aplicacao e criada SEM o lifespan (`create_app()` direto, e nao o
`app` do modulo), porque o lifespan iniciaria o dispatcher -- e um scheduler
materializando tarefas no meio do teste tornaria as contagens
imprevisiveis.

O banco, esse sim, e real: e onde estao as constraints, os enums e o
`ON CONFLICT` que queremos verificar.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from apt.api.deps import AppState
from apt.api.main import create_app

pytestmark = pytest.mark.integration


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Cliente HTTP falando com a app em processo, sem lifespan."""
    from apt.db.engine import check_health, dispose_engine

    if not await check_health():
        pytest.skip("Postgres indisponivel -- suba o stack com `docker compose up -d`")

    app = create_app()
    # O `AppState` normalmente e criado no lifespan. Injetamos um vazio para que
    # as dependencias que o exigem funcionem sem iniciar o dispatcher.
    app.state.apt = AppState()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    await dispose_engine()


def campaign_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": "Campanha de teste",
        "platform": "youtube",
        "total_sends": 50,
        "target_rate_per_min": 120,
        "jitter_strategy": "humanized",
        "contents": [
            {"url": "https://youtube.com/watch?v=t1", "weight": 2},
            {"url": "https://youtube.com/watch?v=t2", "weight": 1},
        ],
        "activate": False,
    }
    payload.update(overrides)
    return payload


class TestCriacao:
    async def test_cria_campanha_em_draft(self, client: httpx.AsyncClient) -> None:
        response = await client.post("/campaigns", json=campaign_payload())
        assert response.status_code == 201

        body = response.json()
        assert body["status"] == "draft"
        assert body["platform"] == "youtube"
        assert body["dispatched_count"] == 0

    async def test_activate_true_ja_nasce_ativa(self, client: httpx.AsyncClient) -> None:
        """Com `activate=True`, a campanha vira `active` na mesma transacao.

        A ativacao acontece DEPOIS do cadastro dos conteudos -- e o que elimina a
        janela em que o dispatcher encontraria uma campanha ativa sem pool.
        """
        response = await client.post("/campaigns", json=campaign_payload(activate=True))
        assert response.status_code == 201
        assert response.json()["status"] == "active"

    async def test_pool_de_conteudos_e_persistido(self, client: httpx.AsyncClient) -> None:
        created = await client.post("/campaigns", json=campaign_payload())
        campaign_id = created.json()["id"]

        status = await client.get(f"/campaigns/{campaign_id}/status")
        assert status.status_code == 200

        contents = status.json()["contents"]
        assert len(contents) == 2
        pesos = {c["content_url"]: c["weight"] for c in contents}
        assert pesos["https://youtube.com/watch?v=t1"] == 2

    async def test_recusa_pool_vazio(self, client: httpx.AsyncClient) -> None:
        """Campanha sem URL nao tem o que enviar -- barramos na entrada.

        Aceita-la produziria uma campanha ativa e inerte, com o dispatcher
        emitindo WARNING a cada tick.
        """
        response = await client.post("/campaigns", json=campaign_payload(contents=[]))
        assert response.status_code == 422

    async def test_recusa_urls_duplicadas(self, client: httpx.AsyncClient) -> None:
        """URL repetida no mesmo pool e recusada com 422.

        O banco tem `UNIQUE (campaign_id, content_url)` e o `ON CONFLICT` do
        repositorio faria a segunda ocorrencia sobrescrever o peso da primeira em
        silencio. Quem mandou a mesma URL com pesos diferentes provavelmente
        errou -- e melhor dizer isso do que escolher um dos pesos por ele.
        """
        response = await client.post(
            "/campaigns",
            json=campaign_payload(
                contents=[
                    {"url": "https://youtube.com/watch?v=igual", "weight": 1},
                    {"url": "https://youtube.com/watch?v=igual", "weight": 5},
                ]
            ),
        )
        assert response.status_code == 422

    @pytest.mark.parametrize(
        ("campo", "valor"),
        [
            ("total_sends", 0),
            ("total_sends", -10),
            ("target_rate_per_min", 0),
            ("target_rate_per_min", -1),
            ("platform", "tiktok"),  # nao suportada nesta POC
            ("jitter_strategy", "aleatoria"),  # estrategia inexistente
        ],
    )
    async def test_recusa_entrada_invalida(
        self, client: httpx.AsyncClient, campo: str, valor: object
    ) -> None:
        """Validacao acontece antes de qualquer codigo nosso rodar.

        `target_rate_per_min = 0` chegaria ate `jitter.plan_tick` e produziria
        divisao estranha; barrar no schema devolve 422 com mensagem clara.
        """
        response = await client.post("/campaigns", json=campaign_payload(**{campo: valor}))
        assert response.status_code == 422


class TestConsulta:
    async def test_detalha_campanha(self, client: httpx.AsyncClient) -> None:
        created = await client.post("/campaigns", json=campaign_payload())
        campaign_id = created.json()["id"]

        response = await client.get(f"/campaigns/{campaign_id}")
        assert response.status_code == 200
        assert response.json()["id"] == campaign_id

    async def test_campanha_inexistente_da_404(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/campaigns/00000000-0000-0000-0000-000000000000")
        assert response.status_code == 404

    async def test_uuid_invalido_da_422(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/campaigns/nao-e-uuid")
        assert response.status_code == 422

    async def test_lista_filtrando_por_status(self, client: httpx.AsyncClient) -> None:
        await client.post("/campaigns", json=campaign_payload(activate=True))
        await client.post("/campaigns", json=campaign_payload(activate=False))

        response = await client.get("/campaigns", params={"status": "active"})
        assert response.status_code == 200
        assert all(c["status"] == "active" for c in response.json())

    async def test_status_traz_progresso(self, client: httpx.AsyncClient) -> None:
        created = await client.post("/campaigns", json=campaign_payload())
        campaign_id = created.json()["id"]

        response = await client.get(f"/campaigns/{campaign_id}/status")
        body = response.json()
        assert body["progress_percent"] == 0.0
        assert "task_breakdown" in body
        assert "outcome_breakdown" in body


class TestPausarRetomar:
    async def test_pausa_e_retoma(self, client: httpx.AsyncClient) -> None:
        created = await client.post("/campaigns", json=campaign_payload(activate=True))
        campaign_id = created.json()["id"]

        paused = await client.post(f"/campaigns/{campaign_id}/pause")
        assert paused.status_code == 200
        assert paused.json()["status"] == "paused"

        resumed = await client.post(f"/campaigns/{campaign_id}/resume")
        assert resumed.status_code == 200
        assert resumed.json()["status"] == "active"

    async def test_pausar_inexistente_da_404(self, client: httpx.AsyncClient) -> None:
        response = await client.post("/campaigns/00000000-0000-0000-0000-000000000000/pause")
        assert response.status_code == 404


class TestHealth:
    async def test_live_sempre_responde(self, client: httpx.AsyncClient) -> None:
        """Liveness nao checa dependencia -- responde 200 se o processo vive."""
        response = await client.get("/health/live")
        assert response.status_code == 200
        assert response.json()["status"] == "alive"

    async def test_ready_reporta_cada_check(self, client: httpx.AsyncClient) -> None:
        """Readiness detalha QUAL verificacao falhou.

        Um health check que devolve apenas o status obriga quem investiga a ir ao
        log para descobrir a causa.
        """
        response = await client.get("/health/ready")
        assert response.status_code in (200, 503)
        checks = response.json()["checks"]
        assert set(checks) == {"postgres", "redis", "dispatcher"}

    async def test_metrics_expoe_formato_prometheus(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/metrics")
        assert response.status_code == 200
        assert "apt_" in response.text
