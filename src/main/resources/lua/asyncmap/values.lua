-- KEYS[1] = Entry key pattern

local values = {}
local keys = redis.call("KEYS", KEYS[1])

for unused, key in ipairs(keys) do
    values[#values + 1] = redis.call("GET", key)
end

return values
