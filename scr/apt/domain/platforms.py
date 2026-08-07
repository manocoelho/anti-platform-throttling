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

- YouTube e Instagram com limites assimetricos (5 vs 10 req/s), para que o
  bulkhead tenha plataformas de perfis diferentes para isolar;
- limites baixos o suficiente para um teste de carga completar em segundos;
- o YouTube calibrado abaixo do teto de vazao agregada do ambiente de teste
  (uma VM compartilhada, ~6-8 req/s), para que o rate limiter -- e nao o
  hardware do harness -- seja o gargalo observado nos testes de carga e
  escala. Ver a nota "RECALIBRACAO DO YOUTUBE" junto de `PLATFORM_PROFILES`.

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
        4. (Especifico do YouTube neste ambiente.) O harness de teste tem um teto
           de vazao agregada proprio (~6-8 req/s nesta VM). Se `allowed_rps` ficar
           proximo desse teto, ele deixa de ser um detalhe do ambiente e passa a
           ser um confundidor da medicao -- ver a nota "RECALIBRACAO DO YOUTUBE"
           junto de `PLATFORM_PROFILES` e TRADE-OFFS.md item 19. E por isso o
           YouTube usa uma margem (40%) maior que a do Instagram (20%): a margem
           extra nao e sobre o limite da plataforma, e sobre o teto do ambiente.

        IMPORTANTE: esta fracao mede apenas a folga da vazao SUSTENTADA
        (`allowed_rps` vs `estimated_limit_rps`). Ela NAO garante nada sobre o
        pior caso de rajada -- um bucket cheio mais o refill do mesmo segundo
        pode somar `burst_capacity + allowed_rps` requisicoes numa unica
        janela de 1s do simulador. A invariante que cobre isso e a de
        `test_domain.py::test_burst_mais_refill_nao_passa_do_limite_estimado`,
        nao esta propriedade. Ver TRADE-OFFS.md item 16.
        """
        if self.estimated_limit_rps <= 0:
            return 0.0
        return 1.0 - (self.allowed_rps / self.estimated_limit_rps)


# Os valores aqui espelham o seed de `db/migrations/001_init.sql`. O banco e a
# fonte de verdade em runtime (o administrador pode ajustar sem redeploy); este
# dicionario e o fallback usado quando o banco ainda nao respondeu e a
# referencia para os testes unitarios, que rodam sem infraestrutura.
#
# CALIBRACAO DO BURST -- por que 1 e 1, e nao 3 e 1
#
# `burst_capacity` e o tamanho do balde do token bucket: quando ele esta
# cheio, um pico de demanda pode consumir as `burst_capacity` fichas quase
# instantaneamente, e o refill do MESMO segundo ainda soma `allowed_rps` a
# esse pico. No pior caso, uma unica janela de 1s do simulador
# (`PlatformThrottle`, janela deslizante de 1s) ve:
#
#     burst_capacity + allowed_rps  requisicoes
#
# A invariante correta e essa soma ficar ABAIXO do limite estimado:
#
#     burst_capacity + allowed_rps <= estimated_limit_rps
#
# Com os valores antigos (burst=16, allowed=16, limite=20): 16+16=32 > 20 --
# o pior caso ESTOURAVA o limite por construcao, nao por desalinhamento de
# janela. Foi exatamente isso que produziu 429 reais mesmo com o rate
# limiter ligado (ver RESULTADOS-TESTES.md, primeira execucao).
#
# Os valores abaixo ficam ABAIXO do teto exato (burst<=2 para YouTube,
# burst<=2 para Instagram), nao NELE: um sistema no teto exato fica na
# fronteira, e qualquer desalinhamento entre a nossa janela e a do simulador
# devolveria 429 -- inclusive numa demonstracao ao vivo. Ver TRADE-OFFS.md
# item 16.
#
# RECALIBRACAO DO YOUTUBE (16/3/20 -> 3/1/5) -- por que so o YouTube mudou
#
# Esta VM compartilhada tem um teto de vazao AGREGADA (todos os workers
# somados) medido em ~6-8 req/s -- ver RESULTADOS-TESTES.md secao 1.6. Com o
# YouTube calibrado em 16 req/s permitidos contra um limite estimado de 20,
# esse teto de hardware ficava ABAIXO do que o rate limiter jamais chegaria a
# restringir: o pico observado nunca alcancava 16, entao nao dava para saber
# se o platô medido era o rate limiter (o mecanismo) ou a VM (o ambiente)
# fazendo o trabalho. Isso e um confundidor -- invalida qualquer
# contrafactual que desligue o rate limiter esperando ver 429 aparecer, e
# deixa duas explicacoes igualmente compativeis para o platô do teste de
# escala (C-3), sem medicao que as separe.
#
# Recalibrar o YOUTUBE para ficar ABAIXO do teto do ambiente (3 permitidos,
# limite estimado 5) remove o confundidor: agora e o rate limiter que
# restringe antes que o teto da VM entre em jogo, o contrafactual (desligar a
# flag) volta a produzir 429 de verdade, e o platô do C-3 passa a coincidir
# com o numero configurado, nao com um teto de hardware nao intencional.
#
# O INSTAGRAM NAO MUDOU DE PROPOSITO. O Cenario B (resiliencia/bulkhead) usa
# o par (8, 1, 10) do Instagram e ja estava validado; alterar esses numeros
# invalidaria aquela rodada sem necessidade -- o bug que motivou esta
# recalibracao (teto de ambiente mascarando o mecanismo) so se manifestava no
# YouTube, porque so o YouTube tinha allowed_rps proximo do teto da VM.
#
# O efeito colateral e intencional: agora as duas plataformas exercitam
# regimes DIFERENTES no mesmo ambiente -- o YouTube fica limitado pelo
# MECANISMO (3 req/s, bem abaixo do teto de ~6-8 do ambiente) e o Instagram
# continua limitado pelo proprio numero configurado (8 req/s), que por
# coincidencia fica no mesmo patamar do teto do ambiente. Essa assimetria de
# regime, e nao so de numero, e o que a POC agora demonstra. Ver
# TRADE-OFFS.md.
#
PLATFORM_PROFILES: dict[Platform, PlatformProfile] = {
    Platform.YOUTUBE: PlatformProfile(
        platform=Platform.YOUTUBE,
        estimated_limit_rps=5.0,
        allowed_rps=3.0,
        burst_capacity=1,
        endpoint_path="/youtube/engagements",
        description=(
            "Calibrado ABAIXO do teto de vazao agregada desta VM (~6-8 req/s) "
            "de proposito: para que o rate limiter, e nao o hardware do "
            "ambiente de teste, seja o gargalo observado. Ver a nota "
            "'RECALIBRACAO DO YOUTUBE' acima."
        ),
    ),
    Platform.INSTAGRAM: PlatformProfile(
        platform=Platform.INSTAGRAM,
        estimated_limit_rps=10.0,
        allowed_rps=8.0,
        burst_capacity=1,
        endpoint_path="/instagram/engagements",
        description=(
            "Limite mantido intencionalmente diferente do YouTube -- a "
            "assimetria e o que torna visivel o efeito do bulkhead: quando "
            "esta plataforma degrada, a outra segue enviando na vazao "
            "normal. Desde a recalibracao do YouTube, os dois perfis tambem "
            "exercitam regimes diferentes: este fica limitado pelo proprio "
            "numero configurado, o YouTube fica limitado bem abaixo do teto "
            "do ambiente."
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
