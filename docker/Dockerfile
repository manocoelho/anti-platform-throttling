# ---------------------------------------------------------------------------
# Imagem unica para os tres processos Python do sistema (api, worker,
# platform-sim). O que muda entre eles e apenas o `command` no
# docker-compose.yml -- o codigo e as dependencias sao os mesmos.
#
# Por que uma imagem so?
#   - Um unico build para manter em cache: subir o stack fica bem mais rapido.
#   - Garante que API e worker rodam exatamente o mesmo codigo de dominio
#     (nao existe risco de versoes divergentes entre servicos).
#   - Trade-off assumido: a imagem do worker carrega o FastAPI que ele nao
#     usa. Custa alguns MB e nao justifica a complexidade de tres builds.
#     Ver docs/TRADE-OFFS.md.
#
# POR QUE NAO HA `apt-get` NESTE ARQUIVO
#
# A primeira versao instalava `gcc`/`libc6-dev` no estagio de build (para o
# caso de o asyncpg precisar compilar) e `curl` no estagio final (para os
# healthchecks). Removemos os dois:
#
#   gcc  -- desnecessario. O asyncpg publica wheels manylinux para CPython 3.12,
#           entao o pip baixa binario pronto. Compilar seria trabalho que nao
#           produz nada de diferente. `--only-binary` (abaixo) torna isso
#           explicito: se algum dia faltar wheel, o build falha com mensagem
#           clara em vez de tentar compilar sem compilador.
#
#   curl -- substituivel. Os healthchecks agora usam `python -c` com
#           `urllib.request`, que ja esta na imagem. Ver docker-compose.yml.
#
# O ganho nao e so de tempo de build. A imagem final passa a nao ter gerenciador
# de pacotes nem compilador -- menos superficie para uma vulnerabilidade de
# dependencia de sistema, e um build que nao depende da disponibilidade dos
# repositorios Debian.
#
# Build multi-stage: o estagio `builder` resolve as dependencias e o estagio
# final leva somente a arvore instalada.
# ---------------------------------------------------------------------------

# --- Estagio 1: resolucao das dependencias ---------------------------------
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Copiamos apenas os metadados e o codigo. Enquanto o pyproject.toml nao mudar,
# o Docker reaproveita a camada de dependencias e o rebuild fica rapido.
COPY pyproject.toml ./
COPY src/ ./src/

# --prefix isola a instalacao numa arvore que copiamos inteira para o estagio
# final. --only-binary=:all: garante que nada e compilado do fonte: sem
# compilador na imagem, uma tentativa de build de fonte falharia de forma
# confusa no meio do processo. Melhor falhar aqui, explicitamente.
RUN pip install --prefix=/install --only-binary=:all: .

# --- Estagio 2: imagem de execucao -----------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/install/bin:${PATH}" \
    PYTHONPATH="/install/lib/python3.12/site-packages"

# Seguranca: o processo nao roda como root. Se um container for comprometido,
# o atacante nao tem privilegio administrativo dentro dele.
# `useradd` vem do coreutils da imagem base -- nao precisa de apt-get.
RUN useradd --create-home --uid 10001 apt

COPY --from=builder /install /install

WORKDIR /app
USER apt

# Sem CMD proposital: cada servico define o seu `command` no compose.
# Deixar isso explicito evita que alguem suba a imagem errada por engano.
