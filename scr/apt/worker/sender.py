"""Cliente HTTP que efetivamente envia as requisicoes as plataformas.

Duas decisoes de projeto que valem explicacao:

UM POOL DE CONEXOES POR PLATAFORMA

Cada plataforma tem o seu proprio `httpx.AsyncClient`, com limites de conexao
proprios. E a terceira camada do Bulkhead:

    fila dedicada    -> isola no BROKER
    semaforo         -> isola os SLOTS DE EXECUCAO do worker
    pool HTTP        -> isola as CONEXOES DE REDE

Sem a terceira, um `AsyncClient` compartilhado teria um pool comum: requisicoes
lentas do Instagram ocupariam conexoes do pool e as do YouTube ficariam
esperando conexao livre -- reintroduzindo o acoplamento que as duas primeiras
camadas eliminaram.

TRADUZIR RESPOSTA HTTP EM `Outcome`

Toda a interpretacao do que a plataforma respondeu acontece aqui e em nenhum
outro lugar. O worker recebe um `Outcome` do enum e nao precisa conhecer codigos
HTTP. Isso mantem a maquina de decisao do worker legivel e concentra num ponto
so a resposta para "429 conta como falha para o circuit breaker?".
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import httpx

from apt.config import get_settings
from apt.domain.models import Outcome, Platform
from apt.domain.platforms import get_profile
from apt.logging_setup import get_correlation_id, get_logger

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SendResult:
    """Resultado de uma tentativa de envio.

    Attributes:
        outcome: resultado normalizado.
        http_status: status devolvido, ou `None` se nao houve resposta.
        latency_ms: tempo total da chamada. Medido inclusive em falha -- saber
            que um timeout levou 5000ms confirma que o timeout configurado foi o
            que interrompeu.
        retry_after_ms: valor do header `Retry-After` convertido para ms, quando
            a plataforma o informou. E uma instrucao explicita e tem precedencia
            sobre o nosso backoff calculado (ver `resilience/retry.py`).
        error: mensagem de erro, quando houver.
    """

    outcome: Outcome
    http_status: int | None = None
    latency_ms: int = 0
    retry_after_ms: int | None = None
    error: str | None = None


class PlatformSender:
    """Envia requisicoes as plataformas, com um pool de conexoes por plataforma."""

    def __init__(self) -> None:
        self._settings = get_settings()
        self._clients: dict[Platform, httpx.AsyncClient] = {}

    def _client_for(self, platform: Platform) -> httpx.AsyncClient:
        """Devolve (criando na primeira vez) o cliente HTTP da plataforma."""
        client = self._clients.get(platform)
        if client is not None:
            return client

        bulkhead_capacity = self._settings.bulkhead.for_platform(platform)
        client = httpx.AsyncClient(
            base_url=self._settings.platform_sim_url,
            timeout=httpx.Timeout(self._settings.send_timeout_seconds),
            limits=httpx.Limits(
                # O pool acompanha a cota do bulkhead: nao ha razao para manter
                # mais conexoes do que envios simultaneos permitidos. Um pool
                # maior seria memoria e file descriptors parados.
                max_connections=bulkhead_capacity,
                max_keepalive_connections=bulkhead_capacity,
                keepalive_expiry=30.0,
            ),
            # Redirect desligado de proposito: um 3xx inesperado deve aparecer
            # como anomalia, nao ser seguido silenciosamente para um destino que
            # nao foi o que pedimos.
            follow_redirects=False,
        )
        self._clients[platform] = client
        logger.info(
            "sender.client_created",
            platform=str(platform),
            max_connections=bulkhead_capacity,
            timeout_seconds=self._settings.send_timeout_seconds,
        )
        return client

    async def send(self, platform: Platform, *, content_url: str, task_id: str) -> SendResult:
        """Envia um engajamento e traduz a resposta em `SendResult`.

        Nunca propaga excecao: qualquer falha de rede vira um `Outcome`. O worker
        precisa sempre poder decidir entre ack, retry e DLQ -- uma excecao
        vazando daqui obrigaria cada ponto de chamada a repetir o mesmo
        tratamento.
        """
        profile = get_profile(platform)
        client = self._client_for(platform)
        started = time.perf_counter()

        try:
            response = await client.post(
                profile.endpoint_path,
                json={
                    "content_url": content_url,
                    "task_id": task_id,
                    # O simulador ecoa este campo nos logs; e o que permite
                    # correlacionar o registro do "lado da plataforma" com o
                    # nosso.
                    "correlation_id": get_correlation_id(),
                },
            )
        except httpx.TimeoutException as exc:
            latency = int((time.perf_counter() - started) * 1000)
            logger.warning(
                "sender.timeout", platform=str(platform), task_id=task_id, latency_ms=latency
            )
            return SendResult(outcome=Outcome.TIMEOUT, latency_ms=latency, error=f"timeout: {exc}")
        except httpx.HTTPError as exc:
            # Cobre erro de conexao, DNS e TLS -- a plataforma esta inalcancavel.
            latency = int((time.perf_counter() - started) * 1000)
            logger.warning(
                "sender.connection_error",
                platform=str(platform),
                task_id=task_id,
                error=str(exc),
            )
            return SendResult(
                outcome=Outcome.ERROR, latency_ms=latency, error=f"erro de rede: {exc}"
            )

        latency = int((time.perf_counter() - started) * 1000)
        return self._interpret(response, platform=platform, latency_ms=latency)

    def _interpret(
        self, response: httpx.Response, *, platform: Platform, latency_ms: int
    ) -> SendResult:
        """Traduz a resposta HTTP num `Outcome`."""
        status_code = response.status_code

        if 200 <= status_code < 300:
            return SendResult(outcome=Outcome.SENT, http_status=status_code, latency_ms=latency_ms)

        if status_code == 429:
            # Este e o resultado que a POC existe para evitar. Log em WARNING
            # porque cada ocorrencia e um sinal de que a calibragem esta
            # otimista -- o nosso `allowed_rps` esta acima do limite real.
            retry_after_ms = self._parse_retry_after(response)
            logger.warning(
                "sender.throttled",
                platform=str(platform),
                status=status_code,
                retry_after_ms=retry_after_ms,
                note="a plataforma nos limitou: revisar allowed_rps",
            )
            return SendResult(
                outcome=Outcome.THROTTLED,
                http_status=status_code,
                latency_ms=latency_ms,
                retry_after_ms=retry_after_ms,
                error="rate limit da plataforma (429)",
            )

        return SendResult(
            outcome=Outcome.ERROR,
            http_status=status_code,
            latency_ms=latency_ms,
            retry_after_ms=self._parse_retry_after(response),
            error=f"HTTP {status_code}: {response.text[:200]}",
        )

    @staticmethod
    def _parse_retry_after(response: httpx.Response) -> int | None:
        """Le o header `Retry-After` e devolve o valor em milissegundos.

        A RFC 9110 permite duas formas: segundos (inteiro) ou data HTTP.
        Tratamos apenas a primeira, que e o que as APIs usam na pratica; uma data
        devolve `None` e o worker cai no backoff calculado -- degradacao
        aceitavel para um formato raro.
        """
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        try:
            return max(0, int(float(raw.strip()) * 1000))
        except (TypeError, ValueError):
            logger.debug("sender.retry_after_unparsed", raw=raw)
            return None

    async def close(self) -> None:
        """Fecha todos os pools de conexao. Chamado no shutdown do worker."""
        for platform, client in self._clients.items():
            await client.aclose()
            logger.debug("sender.client_closed", platform=str(platform))
        self._clients.clear()