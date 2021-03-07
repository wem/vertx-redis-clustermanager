-- KEYS[1] = Entry key pattern

redis.call("DEL", unpack(redis.call("KEYS", KEYS[1])))