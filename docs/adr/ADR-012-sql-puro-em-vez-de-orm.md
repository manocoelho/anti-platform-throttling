# ADR-012 — SQL explícito em vez de ORM, e migrações em SQL puro

**Status:** Aceita
**Origem:** Projeto 03

## Contexto

O Projeto 02 definiu PostgreSQL como banco, sem especificar a camada de acesso. Duas
decisões precisavam ser tomadas: **ORM ou SQL explícito** e **ferramenta de migração
ou SQL versionado**.

O que o sistema realmente pede do banco:

1. **Poucas entidades** — 7 tabelas, nenhuma com relacionamento complexo.
2. **Consultas específicas do Postgres** em pontos críticos:
   - `SELECT ... FOR UPDATE SKIP LOCKED` — reserva de campanhas pelo dispatcher
     (ADR-010)
   - `UPDATE ... RETURNING` numa subquery com `SKIP LOCKED` — rotação atômica do pool
     de URLs
   - `INSERT ... ON CONFLICT DO UPDATE` — cadastro idempotente de conteúdos
   - `percentile_cont(0.95) WITHIN GROUP (ORDER BY ...)` — percentis de latência
   - `date_trunc('second', ...)` com agregação — série de envios por segundo
   - índices **parciais** (`WHERE status = 'active'`) e tipos **ENUM** nativos
3. **Nenhuma tela.** Não há CRUD administrativo que se beneficiaria de scaffolding.

## Decisão

**SQLAlchemy Core** com `text()` e SQL explícito, todo concentrado em
`src/apt/db/repositories.py`. Sem modelos declarativos, sem ORM.

**Migrações em SQL puro** (`db/migrations/001_init.sql`), aplicadas pelo mecanismo
nativo do Postgres: o arquivo é montado em `/docker-entrypoint-initdb.d` e roda na
primeira inicialização do volume.

A regra que sustenta a organização: **nenhum módulo fora de `apt.db` escreve SQL.**
API, worker e scheduler chamam métodos de repositório. Quando uma consulta ficar
lenta, existe um único lugar para procurar.

## Alternativas consideradas

### SQLAlchemy ORM (modelos declarativos)

A escolha padrão do ecossistema. Recusada por um argumento concreto: boa parte das
consultas que importam **viraria `session.execute(text(...))` de qualquer forma**.

`SKIP LOCKED`, `ON CONFLICT DO UPDATE` e `percentile_cont` são expressáveis no ORM,
mas de forma que fica mais verbosa e menos legível que o SQL correspondente — e quem
for revisar precisa conhecer as duas coisas. Manteríamos modelos declarativos
duplicando o schema SQL, pagaríamos o custo de mantê-los sincronizados, e usaríamos
SQL cru nos pontos críticos mesmo assim.

Há um argumento de peso *a favor* do ORM que reconhecemos: com modelos declarativos,
o mypy verificaria os tipos das colunas. No nosso desenho, as linhas voltam como
`dict[str, Any]` e a conversão é manual (`float(str(row["allowed_rps"]))` para lidar
com `Decimal`). É uma perda real de segurança de tipos, aceita em troca de ter uma
única fonte de verdade do schema.

### Alembic para migrações

O padrão do ecossistema SQLAlchemy, e a escolha certa para um sistema com evolução de
schema em produção. Recusado por **escopo**: a POC tem uma migração inicial e nenhuma
evolução prevista. Alembic exigiria diretório de versões, arquivo de configuração,
ambiente de execução e — sem modelos declarativos — a autogeração não funcionaria,
então escreveríamos o SQL na mão dentro dos arquivos de migração.

**Isto é uma limitação assumida, não um argumento de que Alembic é desnecessário.** O
mecanismo atual só roda na *primeira* inicialização do volume: alterar o schema hoje
exige `docker compose down -v` (o que **apaga os dados**). Em qualquer contexto com
dados que importam, Alembic entraria antes da primeira alteração de schema.

### Um cliente Postgres direto (`asyncpg` puro, sem SQLAlchemy)

Seria mais rápido — SQLAlchemy Core adiciona uma camada fina sobre o driver.
Recusado porque o que ganhamos com o SQLAlchemy vale mais que a diferença de
desempenho: pool de conexões com `pool_pre_ping` (que evita entregar conexão que o
Postgres já fechou — acontece sempre que se reinicia o container do banco com o stack
no ar), gerenciamento de transação por context manager (`engine.begin()` faz commit ao
sair e rollback em exceção), e parametrização nomeada uniforme.

## Consequências positivas

- **O SQL é o que está escrito.** Sem tradução intermediária, sem surpresa de
  `SELECT N+1`, sem dúvida sobre qual consulta o ORM gerou.
- **Recursos do Postgres usados diretamente**, sem contorcer nenhuma abstração. O
  `SKIP LOCKED` do dispatcher e o `UPDATE ... RETURNING` da rotação de URLs são
  centrais ao funcionamento do sistema.
- **Uma fonte de verdade do schema:** o arquivo `.sql`. Não há modelo declarativo
  para manter sincronizado.
- **Transações compostas.** Os métodos recebem a `AsyncConnection` como parâmetro em
  vez de abrir a própria transação — o dispatcher cria a tarefa e incrementa o
  contador da campanha **atomicamente**, e um erro no meio não deixa contador furado.
- **O schema documenta o domínio.** Constraints (`CHECK`), ENUMs e comentários no SQL
  são lidos pela banca junto com o código. Um `CHECK (total_sends > 0)` é uma regra de
  negócio expressa onde ela não pode ser burlada.
- **Migração transparente.** O arquivo `.sql` é aplicável por `psql` — é exatamente o
  que o CI faz nos testes de integração, onde os service containers não executam
  `/docker-entrypoint-initdb.d`.

## Consequências negativas

- **Sem verificação de tipos nas colunas.** As linhas são `dict[str, Any]` e a
  conversão é manual. Um nome de coluna escrito errado só falha em runtime. Mitigado
  parcialmente por concentrar tudo num arquivo e pelos testes de integração exercitarem
  cada consulta contra o banco real.
- **`NUMERIC` volta como `Decimal`.** Exige conversão explícita
  (`float(str(row["allowed_rps"]))`), que é feia. É o preço de não ter mapeamento
  declarativo.
- **Migração só na inicialização do volume.** A limitação mais séria: alterar o schema
  hoje exige recriar o volume, o que apaga os dados. Aceitável numa POC local,
  inaceitável com dados reais.
- **SQL repetido entre consultas semelhantes.** As listas de colunas dos `SELECT`
  aparecem mais de uma vez. Um ORM eliminaria isso.
- **Exige que a equipe saiba SQL.** Não é uma desvantagem num curso de Engenharia de
  Sistemas Distribuídos — mas é uma dependência de conhecimento real.

## Como validamos

- `tests/integration/test_api_campaigns.py` roda contra o **Postgres real**, o que
  exercita as constraints, os ENUMs e o `ON CONFLICT`:
  - `test_recusa_entrada_invalida` — verifica que os limites do schema e do Pydantic
    concordam;
  - `test_recusa_urls_duplicadas` — o `UNIQUE (campaign_id, content_url)`;
  - `test_pool_de_conteudos_e_persistido` — o `ON CONFLICT DO UPDATE`.
- O job `integration` do CI aplica `001_init.sql` com `psql -v ON_ERROR_STOP=1`, o que
  garante que o arquivo é sintaticamente válido e aplicável de forma independente do
  Docker Compose.
- `GET /admin/latency` e `GET /admin/throughput` exercitam `percentile_cont` e
  `date_trunc` — as consultas que motivaram a decisão — e são usadas pelos testes de
  carga.
