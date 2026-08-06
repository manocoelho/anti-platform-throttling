"""Bulkhead: isolamento de recursos por plataforma.

O nome vem da engenharia naval. Um navio e dividido em compartimentos estanques
(bulkheads): se um compartimento inunda, os outros continuam secos e o navio
flutua. Sem eles, uma perfuracao em qualquer ponto afunda o casco inteiro.

O QUE ACONTECE SEM BULKHEAD

O Instagram comeca a responder em 5 segundos em vez de 20ms. Cada envio para o
Instagram ocupa uma corrotina do worker por 5 segundos. Como os envios de
YouTube e Instagram compartilham o mesmo pool de execucao, em poucos segundos
todos os slots estao presos esperando o Instagram -- e os envios de YouTube, que
responderiam normalmente, ficam na fila atras deles.

Resultado: uma plataforma degradada derruba a vazao da outra. Falha em cascata.

O QUE O BULKHEAD FAZ

Cada plataforma recebe uma cota fixa e propria de execucoes simultaneas
(`asyncio.Semaphore`). O Instagram pode esgotar os 4 slots dele -- e nao toca nos
8 do YouTube. A degradacao fica contida no compartimento onde nasceu.

Duas camadas trabalham juntas, e vale distinguir:

    fila dedicada por plataforma (messaging/topology.py)
        -> isolamento no BROKER: mil tarefas de Instagram acumuladas nao ficam
           a frente das tarefas de YouTube.

    semaforo por plataforma (este modulo)
        -> isolamento no WORKER: envios lentos de Instagram nao consomem os
           slots de execucao do YouTube.

Uma sem a outra deixa metade do problema em pe.

POR QUE FAIL-FAST EM VEZ DE ESPERA INDEFINIDA

`acquire()` tem timeout. Esgotado o prazo sem vaga, o envio e RECUSADO e volta
para a fila de retry, em vez de esperar. Uma espera sem limite transformaria o
semaforo numa fila invisivel: as tarefas nao apareceriam em lugar nenhum
(nem na fila do RabbitMQ, nem em execucao), o consumo de memoria cresceria
silenciosamente e a latencia observada seria um numero sem significado.

Recusar rapido e devolver a tarefa a fila mantem o estado do sistema visivel.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from apt.config import get_settings
from apt.domain.models import Platform
from apt.domain.platforms import all_platforms
from apt.logging_setup import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class BulkheadStats:
    """Contadores de um compartimento, para observabilidade.

    Attributes:
        capacity: slots totais.
        in_use: slots ocupados agora.
        acquired_total: quantas vezes um slot foi obtido (acumulado).
        rejected_total: quantas vezes o timeout estourou (acumulado). Um valor
            que cresce indica que a cota esta pequena para a carga OU que a
            plataforma esta lenta -- em ambos os casos, o compartimento esta
            fazendo o seu trabalho e contendo o problema.
        max_in_use: pico de ocupacao observado. Se ficar sempre bem abaixo da
            capacidade, a cota pode ser reduzida.
    """

    capacity: int
    in_use: int = 0
    acquired_total: int = 0
    rejected_total: int = 0
    max_in_use: int = 0


class Bulkhead:
    """Compartimento de concorrencia de uma plataforma."""

    def __init__(self, platform: Platform, capacity: int, *, acquire_timeout: float) -> None:
        if capacity <= 0:
            raise ValueError(f"capacidade do bulkhead precisa ser positiva (recebido {capacity})")
        self.platform = platform
        self._semaphore = asyncio.Semaphore(capacity)
        self._timeout = acquire_timeout
        self.stats = BulkheadStats(capacity=capacity)

    async def acquire(self) -> bool:
        """Tenta obter um slot dentro do timeout.

        Returns:
            True se conseguiu (quem chamou DEVE chamar `release()` depois),
            False se o timeout estourou (nao chamar `release()`).

        Nota sobre `asyncio.wait_for` + `Semaphore.acquire`: quando o timeout
        dispara, o `wait_for` cancela a corrotina do `acquire`. O
        `asyncio.Semaphore` trata o cancelamento corretamente -- ele nao fica
        com um "acquire fantasma" pendente que vazaria capacidade. Esse detalhe
        importa: uma implementacao ingenua de semaforo perderia um slot a cada
        timeout e o compartimento se estreitaria com o tempo.
        """
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=self._timeout)
        except TimeoutError:
            self.stats.rejected_total += 1
            logger.warning(
                "bulkhead.rejected",
                platform=str(self.platform),
                capacity=self.stats.capacity,
                in_use=self.stats.in_use,
                timeout_seconds=self._timeout,
            )
            return False

        self.stats.in_use += 1
        self.stats.acquired_total += 1
        self.stats.max_in_use = max(self.stats.max_in_use, self.stats.in_use)
        return True

    def release(self) -> None:
        """Devolve um slot. Deve ser chamado em `finally` apos um acquire bem-sucedido."""
        self._semaphore.release()
        # `max(0, ...)` protege contra release em excesso por bug de chamada.
        # Sem o clamp, o contador ficaria negativo e a metrica de ocupacao
        # passaria a mentir de forma persistente.
        self.stats.in_use = max(0, self.stats.in_use - 1)

    @property
    def available(self) -> int:
        """Slots livres agora."""
        return max(0, self.stats.capacity - self.stats.in_use)


@dataclass(slots=True)
class BulkheadRegistry:
    """Conjunto dos compartimentos deste worker, um por plataforma.

    Um registro por processo. Os semaforos sao locais ao worker de proposito: o
    bulkhead limita os recursos DESTE processo (corrotinas, conexoes HTTP), e
    esses recursos sao locais. Distribuir esse limite via Redis nao faria
    sentido -- e o rate limiter, que sim precisa de visao global, ja e
    distribuido.
    """

    compartments: dict[Platform, Bulkhead] = field(default_factory=dict)

    @classmethod
    def from_settings(cls) -> BulkheadRegistry:
        """Cria um compartimento por plataforma, com as cotas do `.env`."""
        settings = get_settings()
        cfg = settings.bulkhead
        compartments: dict[Platform, Bulkhead] = {}
        for platform in all_platforms():
            capacity = cfg.for_platform(platform)
            compartments[platform] = Bulkhead(
                platform, capacity, acquire_timeout=cfg.acquire_timeout
            )
            logger.info(
                "bulkhead.compartment_created",
                platform=str(platform),
                capacity=capacity,
                acquire_timeout=cfg.acquire_timeout,
            )
        return cls(compartments=compartments)

    def get(self, platform: Platform) -> Bulkhead:
        try:
            return self.compartments[platform]
        except KeyError as exc:  # pragma: no cover - defensivo
            raise KeyError(
                f"nenhum bulkhead configurado para '{platform}'. Enviar sem "
                "compartimento eliminaria o isolamento entre plataformas."
            ) from exc

    def snapshot(self) -> dict[str, dict[str, int]]:
        """Estado de todos os compartimentos, para a API e as metricas."""
        return {
            str(platform): {
                "capacity": bh.stats.capacity,
                "in_use": bh.stats.in_use,
                "available": bh.available,
                "acquired_total": bh.stats.acquired_total,
                "rejected_total": bh.stats.rejected_total,
                "max_in_use": bh.stats.max_in_use,
            }
            for platform, bh in self.compartments.items()
        }
