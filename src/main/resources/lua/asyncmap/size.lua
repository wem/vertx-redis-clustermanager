-- KEYS[1] = Entry key pattern

return table.getn(redis.call("KEYS", KEYS[1]))