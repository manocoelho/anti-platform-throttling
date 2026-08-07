# ADR-002 — FastAPI como framework backend

**Status:** Aceita
**Origem:** Projeto 02 — **revisado no Projeto 03**

> **Nota sobre a revisão.** O Projeto 02 listava como consequência negativa
> "ecossistema menor que frameworks mais consolidados". Na prática isso **não se
> materializou**: todas as bibliotecas de que precisamos (`asyncpg`, `redis`,
> `aio-pika`, `httpx`, `prometheus-client`) são async-first e independentes do
> framework web. A consequência negativa que **de fato** apareceu é outra, e está
> registrada abaixo: async é fácil de escrever e difícil de acertar — uma chamada
> bloqueante numa corrotina degrada o processo inteiro sem gerar erro.

## Contexto

O sistema precisa de uma API REST para gerenciar campanhas e expor o estado dos
mecanismos de resiliência. Duas restrições pesam na escolha:

1. **O trabalho é I/O-bound, não CPU-bound.** Praticamente todo o tempo de
   execução é passado esperando: Postgres, Redis, RabbitMQ, plataformas. Um
   modelo síncrono com threads gastaria memória mantendo pilhas paradas.
2. **A API compartilha código com o worker.** Ambos usam os mesmos repositórios e
   o mesmo publisher. Se a API fosse síncrona e o worker assíncrono, teríamos duas
   versões de cada camada de acesso a dados.

## Decisão

Adotamos **FastAPI** com **uvicorn**, e o sistema é assíncrono de ponta a ponta:
`asyncpg`, `redis.asyncio`, `aio-pika`, `httpx.AsyncClient`.

## Alternativas consideradas

**Flask.** O framework mais conhecido do ecossistema, e o que a equipe já
conhecia. Recusado por ser síncrono por natureza: cada requisição ocupa uma
thread durante a espera de I/O. Com 8 workers Gunicorn, 8 requisições
concorrentes esgotariam a capacidade — e nossa carga é quase inteiramente espera.
Além disso, validação e documentação seriam trabalho manual (marshmallow +
apispec) em vez de saírem dos type hints.

**Django + DRF.** Traz ORM, admin, migrações e autenticação prontos. É a escolha
certa para um produto com muitas entidades e CRUD. Aqui seria peso morto: temos 7
tabelas, nenhuma tela, e as consultas que importam usam recursos específicos do
Postgres que o ORM não cobre bem (ADR-012). O admin do Django seria útil, mas não
justifica arrastar o framework inteiro.

**Litestar.** Async-first, com API muito parecida com a do FastAPI e desempenho
comparável. Foi a alternativa mais próxima de ser escolhida. Decidimos pelo
FastAPI por uma razão prática de projeto acadêmico: volume de material de
referência e familiaridade da banca. Numa escolha puramente técnica, seria um
empate.

## Consequências positivas

- **Um modelo de concorrência para todo o sistema.** API, scheduler e worker usam
  o mesmo event loop e as mesmas bibliotecas. A camada de repositórios é
  literalmente a mesma.
- **OpenAPI sai de graça.** `/docs` é gerado dos type hints e dos schemas
  Pydantic. Na apresentação, é a demo mais rápida de fazer.
- **Validação antes do nosso código rodar.** Um `target_rate_per_min` negativo é
  rejeitado com 422 e mensagem clara, em vez de virar uma divisão estranha dentro
  do `jitter.plan_tick`.
- **`lifespan` é o lugar natural do scheduler.** Foi o que viabilizou o ADR-010:
  subir o dispatcher junto com a aplicação e derrubá-lo ordenadamente.
- **Injeção de dependências facilita teste.** Os testes de integração sobrescrevem
  dependências em vez de fazer monkeypatch em variável global de módulo.

## Consequências negativas

- **Async é fácil de escrever e difícil de acertar.** Uma única chamada
  bloqueante dentro de uma corrotina (um `requests.get`, um `time.sleep`, uma
  consulta com driver síncrono) congela o event loop inteiro — e não gera erro
  nenhum. O sintoma é latência alta sem causa aparente. É o custo real desta
  escolha, e por isso o projeto não tem **nenhuma** dependência síncrona de I/O.
- **Rastreamento de erro em corrotina é pior.** Um traceback assíncrono atravessa
  o event loop e perde parte do contexto. Foi para compensar isso que existe o
  `correlation_id` propagado por `ContextVar` (`src/apt/logging_setup.py`).
- **`ContextVar` em vez de variável global, obrigatoriamente.** Duas requisições
  concorrentes no mesmo processo compartilham memória de módulo. Estado global
  embaralharia dados entre requisições — um bug que só aparece sob concorrência.
- **A API e o scheduler compartilham o event loop.** Um bug no dispatcher pode
  degradar a latência da API. Consequência direta do ADR-010, aceita
  explicitamente lá.

## Como validamos

- `tests/integration/test_api_campaigns.py::TestCriacao::test_recusa_entrada_invalida`
  — comprova que a validação barra entrada inválida antes de chegar ao domínio.
- `tests/integration/test_api_campaigns.py::TestHealth` — verifica a distinção
  entre liveness e readiness, que depende do `lifespan`.
- O `/docs` gerado, com 21 rotas documentadas, é a evidência do OpenAPI
  automático.
