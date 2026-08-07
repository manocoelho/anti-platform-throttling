--[[
  token_bucket.lua -- consumo atomico de fichas de um token bucket no Redis.

  ESTE E O CORACAO DA POC. Vale entender por que ele existe.

  O PROBLEMA QUE ELE RESOLVE

  Com N workers concorrentes, a versao ingenua do rate limiter faz:

      tokens = GET bucket          -- (1) le
      if tokens >= 1 then
          SET bucket (tokens - 1)  -- (2) escreve
          envia()
      end

  Entre (1) e (2) existe uma janela. Cinco workers podem ler "resta 1 ficha" ao
  mesmo tempo, todos concluirem que podem enviar, e cinco requisicoes saem
  quando havia orcamento para uma. E a condicao de corrida classica de
  read-modify-write, e ela aparece exatamente sob carga -- quando o limite
  importa.

  Escalar workers pioraria o problema: mais concorrencia, mais estouro. Ou seja,
  o sistema violaria o limite justamente quando estivesse fazendo o seu
  trabalho.

  POR QUE LUA RESOLVE

  O Redis executa cada script Lua de forma atomica: enquanto ele roda, nenhum
  outro comando e processado. Ler, calcular o refill, decidir e gravar
  acontecem como uma operacao indivisivel. Nao existe janela.

  Bonus: uma unica ida e volta na rede em vez de duas.

  POR QUE NAO BASTA `DECR`

  `DECR` e atomico, mas nao sabe fazer refill baseado em tempo, nem limitar o
  saldo a uma capacidade maxima, nem calcular quanto tempo falta para a proxima
  ficha. Daria um contador por janela fixa -- que sofre do problema de borda:
  um cliente pode enviar `limite` requisicoes no fim de uma janela e `limite` no
  inicio da seguinte, produzindo 2x o limite em um intervalo curto.

  POR QUE `now_ms` VEM DO CLIENTE

  Poderiamos chamar `redis.call('TIME')`. Preferimos receber `now_ms` como
  argumento por dois motivos:

  1. Testabilidade -- o teste de paridade injeta o mesmo timestamp aqui e na
     implementacao Python de referencia e compara os resultados. Com `TIME`
     interno, as duas nunca coincidiriam exatamente.
  2. Historico de replicacao -- scripts que leem o relogio do servidor eram
     considerados nao deterministicos em versoes mais antigas do Redis, o que
     limitava a replicacao.

  O custo e depender do relogio dos clientes. Se dois workers tiverem relogios
  defasados, o refill fica levemente impreciso. A protecao esta no clamp de
  `elapsed_ms` a zero (mesma protecao existe em token_bucket.py): o desvio pode
  tornar o limite um pouco mais CONSERVADOR, nunca mais permissivo. Errar para o
  lado seguro e a escolha certa aqui.

  CONTRATO

    KEYS[1] = chave do balde (hash com os campos `tokens` e `ts`)
    ARGV[1] = capacity     (numero de fichas, float)
    ARGV[2] = refill_rps   (fichas por segundo, float)
    ARGV[3] = now_ms       (epoch em ms, inteiro)
    ARGV[4] = requested    (fichas a consumir, float)
    ARGV[5] = ttl_seconds  (expiracao da chave, inteiro)

    retorno = { allowed, tokens_milli, retry_after_ms }
      allowed       0 ou 1
      tokens_milli  fichas restantes * 1000, como INTEIRO (ver nota abaixo)
      retry_after_ms 0 quando permitido; senao, ms ate haver ficha

  NOTA SOBRE `tokens_milli`

  O protocolo do Redis converte numeros de retorno para inteiro, truncando a
  parte fracionaria. Devolver `0.85` fichas chegaria ao cliente como `0`.
  Multiplicamos por 1000 e o cliente divide de volta -- preserva tres casas
  decimais, que e mais que suficiente. A alternativa (devolver string e parsear)
  seria mais lenta e mais fragil.

  NOTA SOBRE O TTL

  A chave expira depois de `ttl_seconds` sem uso. E uma faxina automatica: o
  limite por conteudo cria uma chave por URL, e sem TTL o Redis acumularia
  chaves de campanhas encerradas para sempre. Perder um balde inativo e
  inofensivo -- ele renasce cheio, e um balde sem uso recente estaria cheio de
  qualquer forma.
--]]

local capacity  = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local now_ms    = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl       = tonumber(ARGV[5])

-- Validacao defensiva: rate == 0 causaria divisao por zero no calculo de
-- retry_after. Falhar com erro claro e melhor que devolver `inf` ou `nan`, que
-- viajariam pelo sistema e explodiriam num ponto distante da causa.
if capacity == nil or capacity <= 0 then
  return redis.error_reply("token_bucket: capacity precisa ser positiva")
end
if rate == nil or rate <= 0 then
  return redis.error_reply("token_bucket: refill_rps precisa ser positivo")
end

-- HMGET numa unica chamada: dois campos, uma operacao.
local stored = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(stored[1])
local ts     = tonumber(stored[2])

-- Balde inexistente nasce CHEIO. Nascer vazio faria a primeira requisicao de
-- cada URL nova esperar sem motivo (ver token_bucket.py, funcao `consume`).
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now_ms
end

-- Refill proporcional ao tempo decorrido, limitado pela capacidade.
-- O clamp em zero protege contra relogio de cliente andando para tras.
local elapsed_ms = now_ms - ts
if elapsed_ms < 0 then
  elapsed_ms = 0
end
tokens = math.min(capacity, tokens + (elapsed_ms / 1000.0) * rate)

local allowed = 0
local retry_after_ms = 0

if requested > capacity then
  -- Pedido maior que o balde jamais sera atendido. Negamos com retry_after=0
  -- para o chamador nao entrar em espera perpetua.
  allowed = 0
  retry_after_ms = 0
elseif tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  local deficit = requested - tokens
  -- +1 ms para nunca devolver um prazo que expira antes da ficha existir; sem
  -- isso o cliente volta um instante cedo demais e e negado de novo.
  retry_after_ms = math.floor(deficit / rate * 1000.0) + 1
end

-- Gravamos o estado inclusive quando a requisicao foi negada: o `ts` atualizado
-- mantem o refill correto. Mas as fichas NAO sao debitadas numa negativa --
-- negar nao deve cobrar credito, senao um cliente insistente atrasaria os
-- demais.
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
redis.call('EXPIRE', KEYS[1], ttl)

return { allowed, math.floor(tokens * 1000), retry_after_ms }
