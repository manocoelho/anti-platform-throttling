--[[
  circuit_breaker.lua -- transicoes atomicas do circuit breaker distribuido.

  POR QUE O ESTADO E COMPARTILHADO

  Um circuit breaker por processo e o que quase toda biblioteca oferece, e nao
  serve aqui. Com 5 workers e `failure_threshold=5`, cada worker precisaria
  observar 5 falhas por conta propria: 25 requisicoes falhas antes do primeiro
  circuito abrir -- e os outros quatro continuariam martelando a plataforma
  enquanto isso.

  Pior: no caso especifico de throttling, cada requisicao extra durante a
  penalidade tende a estender a penalidade. O breaker por processo produziria
  exatamente o comportamento que a POC existe para evitar.

  Com o estado no Redis, as 5 falhas sao contadas COLETIVAMENTE. O quinto erro
  -- visto por qualquer worker -- abre o circuito para todos, de uma vez.

  POR QUE LUA

  As transicoes sao read-modify-write sobre varios campos. Sem atomicidade:

  - dois workers leem `failure_count = 4`, ambos incrementam, ambos gravam 5:
    uma falha se perde na contagem;
  - em half_open, tres workers leem `probes_in_flight = 1` com limite 2, todos
    concluem que ha vaga, e as tres sondas viram uma rajada sobre um servico
    que acabou de voltar.

  O script resolve as duas coisas: dentro dele, nada mais executa no Redis.

  CONTRATO

    KEYS[1] = chave do circuito (hash)
    ARGV[1] = operacao: "allow" | "success" | "failure"
    ARGV[2] = now_ms              (epoch em ms)
    ARGV[3] = failure_threshold
    ARGV[4] = open_seconds
    ARGV[5] = half_open_probes
    ARGV[6] = success_threshold
    ARGV[7] = ttl_seconds

    retorno = { allowed, state, retry_after_ms, failure_count,
                transitioned, from_state }
      allowed        0/1 (sempre 1 em "success"/"failure"; so "allow" decide)
      state          "closed" | "open" | "half_open" -- estado APOS a operacao
      retry_after_ms 0 quando permitido
      failure_count  falhas consecutivas
      transitioned   0/1 -- houve mudanca de estado nesta chamada
      from_state     estado anterior (relevante quando transitioned = 1)

  A logica aqui espelha `breaker_state.py`, que e a implementacao de referencia
  documentada. Este arquivo comenta apenas o que e especifico do Lua/Redis.
--]]

local key               = KEYS[1]
local op                = ARGV[1]
local now_ms            = tonumber(ARGV[2])
local failure_threshold = tonumber(ARGV[3])
local open_seconds      = tonumber(ARGV[4])
local half_open_probes  = tonumber(ARGV[5])
local success_threshold = tonumber(ARGV[6])
local ttl               = tonumber(ARGV[7])

-- Le o estado atual. Circuito inexistente = fechado e sem falhas, que e o
-- default correto: na duvida, deixe o trafego passar.
local stored = redis.call('HMGET', key, 'state', 'failures', 'successes', 'opened_at', 'probes')
local state     = stored[1] or 'closed'
local failures  = tonumber(stored[2]) or 0
local successes = tonumber(stored[3]) or 0
local opened_at = tonumber(stored[4]) or 0
local probes    = tonumber(stored[5]) or 0

local from_state = state
local allowed = 1
local retry_after_ms = 0
local transitioned = 0

local function persist()
  redis.call('HSET', key,
    'state', state,
    'failures', failures,
    'successes', successes,
    'opened_at', opened_at,
    'probes', probes)
  -- TTL renovado a cada operacao. Um circuito sem trafego nenhum acaba
  -- expirando e volta a "closed" -- que e o comportamento desejado: sem
  -- informacao recente, o padrao e permitir.
  redis.call('EXPIRE', key, ttl)
end

-- =========================================================================
-- allow: "posso enviar agora?"
-- =========================================================================
if op == 'allow' then
  if state == 'closed' then
    allowed = 1

  elseif state == 'open' then
    local elapsed = now_ms - opened_at
    local cooldown = open_seconds * 1000

    if elapsed < cooldown then
      allowed = 0
      retry_after_ms = cooldown - elapsed
      if retry_after_ms < 0 then retry_after_ms = 0 end
    else
      -- Cooldown cumprido: primeira sonda da janela de recuperacao.
      state = 'half_open'
      successes = 0
      probes = 1
      transitioned = 1
      allowed = 1
    end

  else -- half_open
    if probes < half_open_probes then
      probes = probes + 1
      allowed = 1
    else
      -- Cota de sondas esgotada: espere o resultado das que estao em voo.
      allowed = 0
      retry_after_ms = 1000
    end
  end

-- =========================================================================
-- success: uma tentativa deu certo
-- =========================================================================
elseif op == 'success' then
  if state == 'closed' then
    -- Zera as falhas: o gatilho e "N falhas CONSECUTIVAS".
    failures = 0
    successes = 0

  elseif state == 'half_open' then
    successes = successes + 1
    probes = probes - 1
    if probes < 0 then probes = 0 end

    if successes >= success_threshold then
      state = 'closed'
      failures = 0
      successes = 0
      probes = 0
      transitioned = 1
    end
  end
  -- state == 'open': resposta de um envio que ja estava em voo quando o
  -- circuito abriu. Ignorada de proposito -- e informacao mais antiga que a
  -- decisao de abrir, e nao deve ser lida como sinal de recuperacao.

-- =========================================================================
-- failure: uma tentativa falhou (429, 5xx ou timeout)
-- =========================================================================
elseif op == 'failure' then
  if state == 'half_open' then
    -- Falha na sondagem reabre imediatamente e reinicia o cooldown.
    state = 'open'
    failures = failure_threshold
    successes = 0
    probes = 0
    opened_at = now_ms
    transitioned = 1

  elseif state == 'closed' then
    failures = failures + 1
    successes = 0
    if failures >= failure_threshold then
      state = 'open'
      opened_at = now_ms
      probes = 0
      transitioned = 1
    end
  end
  -- state == 'open': falha atrasada. NAO reinicia o cooldown -- reiniciar a
  -- cada resposta tardia poderia manter o circuito aberto para sempre, mesmo
  -- depois da plataforma ter voltado ao normal.

else
  return redis.error_reply("circuit_breaker: operacao invalida '" .. tostring(op) .. "'")
end

persist()

return { allowed, state, retry_after_ms, failures, transitioned, from_state }
