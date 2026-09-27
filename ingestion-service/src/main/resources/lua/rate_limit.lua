-- KEYS[1] = bucket key
-- ARGV[1] = limit, ARGV[2] = window_seconds
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
    local ttl = redis.call("TTL", KEYS[1])
    return {0, ttl}
end
return {1, -1}