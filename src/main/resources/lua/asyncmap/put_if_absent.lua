-- KEYS[1] = Hash key
-- ARGV[1] = New value
-- ARGV[2] = TTL in millis (optional)

local currentValue = redis.call("GET", KEYS[1])

if currentValue == nil or currentValue == false then
    if table.getn(ARGV) == 2 then
        redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
    else
        redis.call("SET", KEYS[1], ARGV[1])
    end
end

return currentValue