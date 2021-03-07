-- KEYS[1] = Hash key
-- ARGV[1] = Value to compare as condition to remove (optional) -> removeIfPresent


local currentValue = redis.call("GET", KEYS[1])

if table.getn(ARGV) == 1 then
    if ARGV[1] == currentValue then
        redis.call("DEL", KEYS[1])
        return "t"
    else
        return "f"
    end
else
    redis.call("DEL", KEYS[1])
    return currentValue
end


