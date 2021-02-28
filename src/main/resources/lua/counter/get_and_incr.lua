-- KEYS[1] = Counter key

local currentValue = redis.call("GET", KEYS[1])

redis.call("INCR", KEYS[1])

if currentValue == nil or currentValue == false then
    return "0"
else
    return currentValue
end