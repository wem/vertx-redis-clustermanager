package ch.sourcemotion.vertx.redis.clustermanager.impl

import ch.sourcemotion.vertx.redis.clustermanager.RedisClusterManagerException
import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaExecutor
import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaScript
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.shareddata.Lock
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request
import mu.KLogging

internal inline class RedisLockName(private val value: String) {
    override fun toString() = value
}

internal class RedisLockFactory(
    private val vertx: Vertx,
    private val redis: Redis,
    private val luaExecutor: LuaExecutor,
    private val lockAcquisitionRetryIntervalMillis: Long,
    private val lockExpirationMillis: Long
) {

    private companion object {
        val acquireLockScript = LuaScript("/lua/lock/acquire-lock.lua")
    }

    fun createLock(name: RedisLockName, timeout: Long, promise: Promise<Lock>) {
        val timeoutThreshold = System.currentTimeMillis() + timeout
        tryLockAcquisition(name, timeout, timeoutThreshold, promise)
    }

    private fun tryLockAcquisition(
        lockName: RedisLockName,
        timeout: Long,
        timeoutThreshold: Long,
        promise: Promise<Lock>
    ) {
        if (System.currentTimeMillis() > timeoutThreshold) {
            promise.fail(RedisClusterManagerException("Unable to acquire Redis cluster lock \"$lockName\" within \"$timeout\" millis"))
        }
        luaExecutor.execute(acquireLockScript, listOf("$lockName"), listOf("$lockExpirationMillis"))
            .onComplete {
                if (it.succeeded() && it.result().isOk()) {
                    promise.complete(RedisLock(redis, lockName))
                } else {
                    vertx.setTimer(lockAcquisitionRetryIntervalMillis) {
                        tryLockAcquisition(lockName, timeout, timeoutThreshold, promise)
                    }
                }
            }
    }
}

internal class RedisLock(private val redis: Redis, private val lockName: RedisLockName) : Lock {

    private companion object : KLogging()

    override fun release() {
        redis.send(Request.cmd(Command.DEL).arg("$lockName"))
            .onSuccess { logger.info { "Vert.x lock \"$lockName\" released" } }
            .onFailure {
                logger.warn(it) {
                    "Release of Vert.x lock \"$lockName\" failed, " +
                            "but should be removed after configured lock expiration."
                }
            }
    }
}

