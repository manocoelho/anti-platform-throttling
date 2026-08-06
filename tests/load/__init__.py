"""Testes de carga, resiliencia e escala.

Nao usam pytest: sao scripts executaveis, porque o produto deles e um RELATORIO
em markdown (que vai para `docs/RESULTADOS-TESTES.md`), nao um verde/vermelho.
Ainda assim, cada um avalia criterios de aceite explicitos e devolve exit code
diferente de zero quando algum falha.

    python -m tests.load.load_test        vazao, latencia p50/p95/p99, taxa de 429
    python -m tests.load.resilience_test  circuit breaker abrindo/fechando + bulkhead
    python -m tests.load.scale_test       1 -> 3 -> 5 workers com limite global constante

`scale_test` e o mais importante: ele e a prova de que o rate limiter e
DISTRIBUIDO. Um limiter em memoria de processo passaria em qualquer teste com um
worker e falharia com cinco -- enviando 5x o limite da plataforma. Ver o
docstring daquele modulo.

Pre-requisito: o stack no ar (`docker compose up -d`). O `scale_test` tambem
executa `docker compose` para alterar o numero de replicas.
"""
