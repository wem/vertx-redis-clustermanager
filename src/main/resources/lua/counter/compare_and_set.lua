-- KEYS[1] = Counter key
-- ARGS[1] = Expected value
-- ARGS[2] = New value

local currentValue = redis.call("GET", KEYS[1])

if currentValue == ARGV[1] then
    redis.call("SET", KEYS[1], ARGV[2])
    return "t"
else
    return "f"
end