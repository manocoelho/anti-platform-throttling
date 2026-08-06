"""Testes de integracao -- exigem Postgres, Redis e RabbitMQ no ar.

    docker compose up -d
    pytest tests/integration -v -m integration

Sem a infraestrutura, fazem SKIP em vez de falhar. A distincao importa: falha
significa "o codigo esta errado", skip significa "nao foi possivel verificar
aqui".

O que so da para verificar contra a infraestrutura real:

    test_rate_limiter_redis.py     atomicidade do script Lua sob concorrencia e
                                   PARIDADE entre a implementacao Lua e a de
                                   referencia em Python
    test_circuit_breaker_redis.py  contagem COLETIVA de falhas entre processos
    test_messaging.py              filas de retry com TTL devolvendo a mensagem
                                   para a fila correta; entrega por fanout
    test_api_campaigns.py          constraints, enums e ON CONFLICT do Postgres
"""
