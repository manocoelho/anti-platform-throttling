"""Configuracao central do sistema, carregada de variaveis de ambiente.

Toda a configuracao passa por aqui. Nenhum outro modulo le `os.environ`
diretamente -- isso garante que existe um unico lugar para descobrir o que e
configuravel, e que um nome de variavel escrito errado falha no boot com
mensagem clara em vez de virar `None` no meio de um envio.

As variaveis usam o prefixo `APT_`. Ver `.env.example` para a lista completa
comentada.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from apt.domain.models import Platform


class RateLimitConfig(BaseSettings):
    """Parametros do token bucket por plataforma e por conteudo.

    Dois eixos de limitacao trabalham juntos:

    - por plataforma: protege contra estourar o limite global da plataforma;
    - por conteudo (URL): impede que uma unica URL consuma toda a cota da
      plataforma. Concentrar volume numa URL e exatamente o padrao que os
      algoritmos de deteccao procuram, entao limitar so no eixo da plataforma
      resolveria metade do problema.
    """

    model_config = SettingsConfigDict(env_prefix="APT_RL_", extra="ignore")

    youtube_rps: float = 16.0
    youtube_burst: int = 16
    instagram_rps: float = 8.0
    instagram_burst: int = 8

    per_content_rps: float = 4.0
    per_content_burst: int = 4

    def for_platform(self, platform: Platform) -> tuple[float, int]:
        """Devolve `(refill_rps, burst_capacity)` da plataforma.

        `refill_rps` e a vazao sustentada; `burst_capacity` e o tamanho maximo
        da rajada instantanea que o bucket tolera quando esta cheio.
        """
        match platform:
            case Platform.YOUTUBE:
                return self.youtube_rps, self.youtube_burst
            case Platform.INSTAGRAM:
                return self.instagram_rps, self.instagram_burst
        # Enum exaustivo; se um valor novo for adicionado sem atualizar aqui,
        # falhamos alto em vez de aplicar silenciosamente um limite errado.
        raise ValueError(f"plataforma sem configuracao de rate limit: {platform}")


class CircuitBreakerConfig(BaseSettings):
    """Parametros do circuit breaker (um circuito independente por plataforma)."""

    model_config = SettingsConfigDict(env_prefix="APT_CB_", extra="ignore")

    # Falhas consecutivas (429, 5xx ou timeout) que abrem o circuito.
    failure_threshold: int = 5
    # Quanto tempo o circuito fica aberto antes de admitir uma sonda.
    open_seconds: int = 15
    # Sondas simultaneas permitidas em half_open. Mais de uma acelera a
    # recuperacao; muitas transformam a sonda numa nova rajada.
    half_open_probes: int = 2
    # Sucessos consecutivos em half_open que fecham o circuito.
    success_threshold: int = 3


class BulkheadConfig(BaseSettings):
    """Limite de concorrencia por plataforma, dentro de cada worker.

    Este e o padrao Bulkhead: cada plataforma recebe a sua propria cota de
    slots de execucao. Se o Instagram comeca a responder em 5 segundos, os
    envios do Instagram esgotam apenas os slots do Instagram -- os do YouTube
    continuam livres.
    """

    model_config = SettingsConfigDict(env_prefix="APT_BULKHEAD_", extra="ignore")

    youtube: int = 8
    instagram: int = 4
    # Tempo maximo aguardando um slot. Estourado, o envio e recusado (fail
    # fast) e reagendado -- prender a tarefa esperando so aumentaria a fila.
    acquire_timeout: float = 2.0

    def for_platform(self, platform: Platform) -> int:
        match platform:
            case Platform.YOUTUBE:
                return self.youtube
            case Platform.INSTAGRAM:
                return self.instagram
        raise ValueError(f"plataforma sem configuracao de bulkhead: {platform}")


class Settings(BaseSettings):
    """Configuracao raiz da aplicacao."""

    model_config = SettingsConfigDict(
        env_prefix="APT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Identidade do processo --------------------------------------------
    env: str = "development"
    # Preenchido pelo compose ("api", "worker", "platform-sim"). Entra em todo
    # log e em toda metrica, para distinguir a origem dos eventos.
    service_name: str = "api"
    log_level: str = "INFO"
    log_json: bool = True

    # --- Dependencias externas --------------------------------------------
    database_url: str = "postgresql+asyncpg://apt:apt_local_password@localhost:5432/apt"
    redis_url: str = "redis://localhost:6379/0"
    rabbitmq_url: str = "amqp://apt:apt_local_password@localhost:5672/"
    platform_sim_url: str = "http://localhost:9001"

    # --- Scheduler (roda como background task da API) ----------------------
    dispatch_tick_seconds: float = 1.0
    dispatch_max_batch: int = 200

    # --- Worker ------------------------------------------------------------
    # prefetch=1: o RabbitMQ entrega uma mensagem por vez a cada worker e so
    # manda a proxima depois do ack. E o que produz balanceamento justo -- com
    # prefetch alto, um worker enche o buffer local e os outros ficam ociosos.
    worker_prefetch: int = 1
    max_attempts: int = 4
    send_timeout_seconds: float = 5.0
    metrics_port: int = 9100

    # --- Retry -------------------------------------------------------------
    retry_base_ms: int = 500
    retry_max_ms: int = 120_000

    # --- Simulador de plataformas -----------------------------------------
    # Latencia artificial do simulador. Sem ela, as respostas voltariam em
    # microssegundos e todos os percentis de latencia do relatorio seriam zero --
    # nao daria para demonstrar que a medicao funciona.
    sim_latency_min_ms: int = 5
    sim_latency_max_ms: int = 40

    # --- Subconfiguracoes --------------------------------------------------
    # default_factory porque cada uma le o seu proprio prefixo de ambiente.
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
    bulkhead: BulkheadConfig = Field(default_factory=BulkheadConfig)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_production(self) -> bool:
        return self.env.lower() in {"production", "prod"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Devolve a instancia unica de configuracao.

    O cache existe por dois motivos: evita reparsear o ambiente a cada
    requisicao e garante que todos os modulos veem exatamente os mesmos
    valores. Nos testes, use `get_settings.cache_clear()` depois de alterar
    variaveis de ambiente.
    """
    return Settings()
