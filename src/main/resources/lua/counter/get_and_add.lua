-- KEYS[1] = Counter key
-- ARGS[1] = Value to add

local currentValue = redis.call("GET", KEYS[1])

if currentValue == nil or currentValue == false then
    redis.call("SET", KEYS[1], ARGV[1])
    return "0"
else
    local newValue = tonumber(currentValue) + ARGV[1]
    redis.call("SET", KEYS[1], newValue)
    return currentValue
end