-- KEYS[1] = Hash key
-- ARGV[1] = New value

local currentValue = redis.call("GET", KEYS[1])
if currentValue ~= nil and currentValue ~= false then
    redis.call("SET", KEYS[1], ARGV[1])
end
return currentValue

