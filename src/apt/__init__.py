"""Anti-Platform Throttling (APT).

POC 4 da disciplina de Engenharia de Sistemas Distribuidos (UFPB, 2026.1).

O sistema controla o envio de requisicoes a plataformas externas de forma a
nao disparar os mecanismos de rate limiting e throttling dessas plataformas.
Cinco subpacotes carregam o peso da solucao:

    resilience/   token bucket distribuido, circuit breaker, bulkhead, retry,
                  feature flags -- o nucleo da POC
    messaging/    topologia RabbitMQ (topic + fanout + DLX/DLQ), publisher,
                  consumer
    scheduling/   distribuicao temporal (jitter) e o dispatcher que materializa
                  campanhas em tarefas
    api/          FastAPI: CRUD de campanhas, flags, health, metricas
    worker/       o consumidor que aplica as politicas antes de cada envio

Fora deles, `platform_sim/` simula YouTube e Instagram (com 429 real e injecao
de falhas) e `observability/` concentra as metricas Prometheus.
"""

__version__ = "1.0.0"
