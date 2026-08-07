"""Worker: consumidor que aplica as politicas de resiliencia antes de enviar.

`main.py` orquestra as cinco camadas (flags -> bulkhead -> circuit breaker ->
rate limiter -> envio). A ordem e justificada no docstring daquele modulo -- ela
nao e arbitraria: cada camada e mais barata que a seguinte, e recusar cedo evita
gastar o recurso da proxima.

`sender.py` faz a chamada HTTP e traduz a resposta da plataforma num `Outcome`.
"""

from apt.worker.main import Worker, build_worker_id, main
from apt.worker.sender import PlatformSender, SendResult

__all__ = ["PlatformSender", "SendResult", "Worker", "build_worker_id", "main"]