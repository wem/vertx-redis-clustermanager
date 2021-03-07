-- KEYS[1] = Entry key pattern

local entries = {}
local keys = redis.call("KEYS", KEYS[1])

for unused, key in ipairs(keys) do
    entries[#entries + 1] = key
    entries[#entries + 1] = redis.call("GET", key)
end

return entries
