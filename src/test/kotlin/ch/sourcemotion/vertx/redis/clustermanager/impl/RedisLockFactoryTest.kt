package ch.sourcemotion.vertx.redis.clustermanager.impl

import ch.sourcemotion.vertx.redis.clustermanager.RedisClusterManagerException
import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaExecutor
import ch.sourcemotion.vertx.redis.clustermanager.testing.AbstractRedisTest
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.vertx.core.Promise
import io.vertx.core.shareddata.Lock
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Request
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test

internal class RedisLockFactoryTest : AbstractRedisTest() {
    private companion object {
        val clusterName = ClusterName("cluster")
        val lockName = RedisLockName("redis-lock")
        val lockKey = clusterName.serviceKey(lockName)
    }

    @Test
    internal fun lock_acquisition_successful(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val sut = RedisLockFactory(vertx, clusterName, redis, LuaExecutor(redis), 100, 1000)
        val promise = Promise.promise<Lock>()
        sut.createLock(lockName, 1, promise)
        val future = promise.future()
        future.onSuccess {
            checkpoint.flag()
        }
        future.onFailure { testContext.failNow(it) }
    }

    @Test
    internal fun lock_acquisition_timeout(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val sut = RedisLockFactory(vertx, clusterName, redis, LuaExecutor(redis), 100, 1000)
        // Another client got the lock earlier
        redis.send(Request.cmd(Command.SET).arg(lockKey).arg(1)).await()

        val promise = Promise.promise<Lock>()
        sut.createLock(lockName, 500, promise)
        val future = promise.future()
        future.onSuccess { testContext.failNow("Lock timeout expected") }
        future.onFailure {
            testContext.verify { it.shouldBeInstanceOf<RedisClusterManagerException>() }
            checkpoint.flag()
        }
    }

    @Test
    internal fun lock_expiration(testContext: VertxTestContext) = testContext.async(1) { checkpoint ->
        val expiration = 200L
        val sut = RedisLockFactory(vertx, clusterName, redis, LuaExecutor(redis), 100, expiration)
        val firstLockPromise = Promise.promise<Lock>()
        sut.createLock(lockName, 100, firstLockPromise)
        firstLockPromise.future().await()

        delay(expiration + 10L)

        val secondLockPromise = Promise.promise<Lock>()
        sut.createLock(lockName, 1, secondLockPromise)
        val secondLockFuture = secondLockPromise.future()
        secondLockFuture.onSuccess { checkpoint.flag() }
        secondLockFuture.onFailure { testContext.failNow(it) }
    }

    @Test
    internal fun lock_release(testContext: VertxTestContext) = testContext.async {
        val sut = RedisLockFactory(vertx, clusterName, redis, LuaExecutor(redis), 100, 1000)
        val promise = Promise.promise<Lock>()
        sut.createLock(lockName, 1, promise)
        val lock = promise.future().await()
        redis.send(Request.cmd(Command.GET).arg(lockKey)).await().toInteger().shouldBe(1)
        lock.release()
        delay(200) // Enough time, even for slow machines.
        redis.send(Request.cmd(Command.GET).arg(lockKey)).await().shouldBeNull()
    }
}