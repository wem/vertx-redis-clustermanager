package ch.sourcemotion.vertx.redis.clustermanager.impl

import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaExecutor
import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaScript
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.Counter
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request

internal inline class RedisCounterKey(private val value: String) {
    override fun toString() = value
}

internal class RedisCounterImpl(
    private val redisCounterKey: RedisCounterKey,
    private val redis: Redis,
    private val luaExecutor: LuaExecutor
) : Counter {

    private companion object {
        val compareAndSetScript = LuaScript("/lua/counter/compare_and_set.lua")
        val getAndAddScript = LuaScript("/lua/counter/get_and_add.lua")
        val getAndIncrScript = LuaScript("/lua/counter/get_and_incr.lua")
    }

    override fun get(): Future<Long> {
        return redis.send(Request.cmd(Command.GET).arg("$redisCounterKey")).compose {
            Future.succeededFuture(it?.toLong() ?: 0)
        }
    }

    override fun incrementAndGet(): Future<Long> {
        return redis.send(Request.cmd(Command.INCR).arg("$redisCounterKey")).compose {
            Future.succeededFuture(it.toLong())
        }
    }

    override fun getAndIncrement(): Future<Long> {
        return luaExecutor.execute(getAndIncrScript, listOf("$redisCounterKey")).compose {
            Future.succeededFuture(it.toLong())
        }
    }

    override fun decrementAndGet(): Future<Long> {
        return redis.send(Request.cmd(Command.DECR).arg("$redisCounterKey")).compose {
            Future.succeededFuture(it.toLong())
        }
    }

    override fun addAndGet(value: Long): Future<Long> {
        return redis.send(Request.cmd(Command.INCRBY).arg("$redisCounterKey").arg(value)).compose {
            Future.succeededFuture(it.toLong())
        }
    }

    override fun getAndAdd(value: Long): Future<Long> {
        return luaExecutor.execute(getAndAddScript, listOf("$redisCounterKey"), listOf("$value")).compose {
            Future.succeededFuture(it.toLong())
        }
    }

    override fun compareAndSet(expected: Long, value: Long): Future<Boolean> {
        return luaExecutor.execute(compareAndSetScript, listOf("$redisCounterKey"), listOf("$expected", "$value")).compose {
            Future.succeededFuture(it.toBoolean())
        }
    }
}