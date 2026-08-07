"""Perfis das plataformas externas simuladas.

IMPORTANTE -- sobre os numeros deste arquivo:

Os thresholds abaixo sao ESTIMATIVAS escolhidas para um ambiente controlado de
laboratorio. Nao sao os limites reais e publicados de YouTube ou Instagram, por
duas razoes:

1. As plataformas nao publicam esses limites, e eles mudam sem aviso.
2. Descobri-los empiricamente exigiria enviar trafego real a servicos de
   terceiros, o que esta fora do escopo (e da etica) de um trabalho academico.

O que a POC demonstra e o MECANISMO de respeitar um limite desconhecido com
margem de seguranca, nao a descoberta dos limites de nenhuma plataforma
especifica. Os valores foram calibrados para dois objetivos didaticos:

- YouTube e Instagram com limites assimetricos (20 vs 10 req/s), para que o
  bulkhead tenha plataformas de perfis diferentes para isolar;
- limites baixos o suficiente para um teste de carga completar em segundos.

Ver `docs/adr/ADR-008-simulador-de-plataformas.md`.
"""

from __future__ import annotations

from dataclasses import dataclass

from apt.domain.models import Platform


@dataclass(frozen=True, slots=True)
class PlatformProfile:
    """Perfil completo de uma plataforma.

    Attributes:
        platform: o identificador da plataforma.
        estimated_limit_rps: limite que ATRIBUIMOS a plataforma. E o valor que
            o simulador aplica para decidir quando devolver 429.
        allowed_rps: vazao que NOS nos permitimos. Sempre menor que
            `estimated_limit_rps` -- ver `safety_margin`.
        burst_capacity: capacidade do token bucket, isto e, o tamanho maximo da
            rajada tolerada quando o bucket esta cheio.
        endpoint_path: caminho no simulador que recebe o envio.
        description: texto de apoio para a documentacao e para a API.
    """

    platform: Platform
    estimated_limit_rps: float
    allowed_rps: float
    burst_capacity: int
    endpoint_path: str
    description: str

    @property
    def safety_margin(self) -> float:
        """Fracao do limite estimado que deixamos de usar (0.20 = 20% de folga).

        Por que sobrar margem em vez de usar 100% do limite:

        1. Nossa janela de contagem nao esta alinhada com a da plataforma. Duas
           janelas deslizantes desalinhadas podem, na virada, somar mais
           requisicoes do que qualquer uma delas mediu isoladamente.
        2. O limite estimado e justamente uma estimativa. Se erramos para cima,
           a margem absorve o erro.
        3. Retries consomem cota. Sem folga, um retry legitimo ja estoura.
        """
        if self.estimated_limit_rps <= 0:
            return 0.0
        return 1.0 - (self.allowed_rps / self.estimated_limit_rps)


# Os valores aqui espelham o seed de `db/migrations/001_init.sql`. O banco e a
# fonte de verdade em runtime (o administrador pode ajustar sem redeploy); este
# dicionario e o fallback usado quando o banco ainda nao respondeu e a
# referencia para os testes unitarios, que rodam sem infraestrutura.
PLATFORM_PROFILES: dict[Platform, PlatformProfile] = {
    Platform.YOUTUBE: PlatformProfile(
        platform=Platform.YOUTUBE,
        estimated_limit_rps=20.0,
        allowed_rps=16.0,
        burst_capacity=16,
        endpoint_path="/youtube/engagements",
        description=(
            "Plataforma de maior volume no cenario. Limite mais alto para "
            "exercitar o caminho de alta vazao."
        ),
    ),
    Platform.INSTAGRAM: PlatformProfile(
        platform=Platform.INSTAGRAM,
        estimated_limit_rps=10.0,
        allowed_rps=8.0,
        burst_capacity=8,
        endpoint_path="/instagram/engagements",
        description=(
            "Limite deliberadamente menor que o do YouTube. A assimetria e o "
            "que torna visivel o efeito do bulkhead: quando esta plataforma "
            "degrada, a outra segue enviando na vazao normal."
        ),
    ),
}


def get_profile(platform: Platform) -> PlatformProfile:
    """Devolve o perfil da plataforma.

    Raises:
        KeyError: se a plataforma nao tem perfil cadastrado. Falhar alto aqui e
            proposital: um perfil ausente significaria enviar trafego sem
            limite conhecido, que e exatamente o que a POC existe para evitar.
    """
    try:
        return PLATFORM_PROFILES[platform]
    except KeyError as exc:  # pragma: no cover - defensivo
        raise KeyError(
            f"plataforma '{platform}' sem perfil em PLATFORM_PROFILES. "
            "Cadastre o perfil antes de enviar trafego para ela."
        ) from exc


def all_platforms() -> tuple[Platform, ...]:
    """Todas as plataformas com perfil cadastrado."""
    return tuple(PLATFORM_PROFILES.keys())
