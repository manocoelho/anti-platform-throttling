# ---------------------------------------------------------------------------
# Atalhos do projeto. Se `make` nao estiver disponivel (comum no Windows),
# cada alvo abaixo mostra o comando equivalente para rodar na mao -- veja
# tambem a secao "Como executar" do README.
# ---------------------------------------------------------------------------
.DEFAULT_GOAL := help
.PHONY: help env up down logs ps restart-workers scale-workers smoke \
        test test-unit test-integration load resilience scale lint fmt types audit clean

help: ## Lista os alvos disponiveis
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

env: ## Cria o .env a partir do .env.example (nao sobrescreve se ja existir)
	@test -f .env || cp .env.example .env
	@echo ".env pronto"

up: env ## Sobe o stack completo em background
	docker compose up -d --build

down: ## Derruba o stack e remove os volumes (apaga o banco)
	docker compose down -v

logs: ## Acompanha os logs de todos os servicos
	docker compose logs -f

ps: ## Mostra o estado de saude de cada servico
	docker compose ps

restart-workers: ## Reinicia apenas os workers (aplica mudanca de codigo/config)
	docker compose restart worker

scale-workers: ## Escala os workers: make scale-workers N=5
	docker compose up -d --scale worker=$(or $(N),3)

smoke: ## Verificacao rapida de que o stack respondeu
	curl -fsS http://localhost:8000/health/ready && echo "" && \
	curl -fsS http://localhost:9001/admin/stats && echo ""

# --- Testes ----------------------------------------------------------------
test: test-unit ## Alias para os testes unitarios (nao exigem infraestrutura)

test-unit: ## Testes unitarios (rodam sem Docker)
	pytest tests/unit -v

test-integration: ## Testes de integracao (exigem o stack no ar)
	pytest tests/integration -v -m integration

load: ## Teste de carga: coleta p50/p95/p99, throughput e taxa de 429
	python -m tests.load.load_test

resilience: ## Teste de resiliencia: injeta falha e observa o circuit breaker
	python -m tests.load.resilience_test

scale: ## Teste de escala: 1 -> 3 -> 5 workers com limite global constante
	python -m tests.load.scale_test

# --- Qualidade -------------------------------------------------------------
lint: ## Verifica estilo e erros estaticos
	ruff check src tests

fmt: ## Formata o codigo
	ruff format src tests
	ruff check --fix src tests

types: ## Checagem de tipos
	mypy

audit: ## Procura vulnerabilidades conhecidas nas dependencias
	pip-audit

clean: ## Remove caches locais
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
