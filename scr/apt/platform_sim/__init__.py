"""Simulador das plataformas externas (YouTube e Instagram).

Existe porque throttling e um fenomeno EMERGENTE -- depende de volume,
espacamento e janela de contagem -- e um mock que devolve 429 sob comando nao o
reproduz. E porque enviar trafego artificial a APIs de terceiros para descobrir
os limites delas nao e uma opcao aceitavel. Ver ADR-008.

Aplica limite por JANELA DESLIZANTE, enquanto o nosso rate limiter usa TOKEN
BUCKET. A assimetria e deliberada: com algoritmos iguais nas duas pontas, o teste
provaria apenas aritmetica. Ver o docstring de `throttle.py`.
"""

from apt.platform_sim.main import app, create_app
from apt.platform_sim.throttle import FaultConfig, FaultMode, PlatformThrottle

__all__ = ["FaultConfig", "FaultMode", "PlatformThrottle", "app", "create_app"]