package ch.sourcemotion.vertx.redis.clustermanager.impl

import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaExecutor
import ch.sourcemotion.vertx.redis.clustermanager.testing.AbstractRedisTest
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.Request
import org.junit.jupiter.api.Test
import kotlin.LazyThreadSafetyMode.NONE

internal class RedisCounterTest : AbstractRedisTest() {

    private companion object {
        val redisCounterKey = RedisCounterKey("redis-counter")
    }

    private val sut by lazy(NONE) { RedisCounterImpl(redisCounterKey, redis, LuaExecutor(redis)) }

    @Test
    internal fun get_no_existing_value(testContext: VertxTestContext) = testContext.async {
        sut.get().await().shouldBe(0)
    }

    @Test
    internal fun get_existing_value(testContext: VertxTestContext) = testContext.async {
        val existingValue = 111L
        setValueAndVerifyResponse(existingValue)
        sut.get().await().shouldBe(existingValue)
    }

    @Test
    internal fun increment_and_get_no_existing_value(testContext: VertxTestContext) = testContext.async {
        sut.incrementAndGet().await().shouldBe(1)
    }

    @Test
    internal fun increment_and_get_existing_value(testContext: VertxTestContext) = testContext.async {
        val existingValue = 100L
        val expectedValue = existingValue + 1
        setValueAndVerifyResponse(existingValue)
        sut.incrementAndGet().await().shouldBe(expectedValue)
    }

    @Test
    internal fun get_and_increment_no_existing_value(testContext: VertxTestContext) = testContext.async {
        sut.andIncrement.await().shouldBe(0)
        sut.get().await().shouldBe(1)
    }

    @Test
    internal fun get_and_increment_existing_value(testContext: VertxTestContext) = testContext.async {
        val existingValue = 2000L
        setValueAndVerifyResponse(existingValue)
        sut.andIncrement.await().shouldBe(existingValue)
        sut.get().await().shouldBe(existingValue + 1)
    }

    @Test
    internal fun decrement_and_get_no_existing_value(testContext: VertxTestContext) = testContext.async {
        sut.decrementAndGet().await().shouldBe(-1)
    }

    @Test
    internal fun decrement_and_get_existing_value(testContext: VertxTestContext) = testContext.async {
        val existingValue = 2000L
        setValueAndVerifyResponse(existingValue)
        sut.decrementAndGet().await().shouldBe(existingValue - 1)
    }

    @Test
    internal fun add_and_get_no_existing_value(testContext: VertxTestContext) = testContext.async {
        sut.addAndGet(1).await().shouldBe(1)
    }

    @Test
    internal fun add_and_get_existing_value(testContext: VertxTestContext) = testContext.async {
        val existingValue = 1000L
        setValueAndVerifyResponse(existingValue)
        val addValue = 1L
        sut.addAndGet(addValue).await().shouldBe(existingValue + addValue)
    }

    @Test
    internal fun get_and_add_no_existing_value(testContext: VertxTestContext) = testContext.async {
        sut.getAndAdd(1).await().shouldBe(0)
        sut.get().await().shouldBe(1)
    }

    @Test
    internal fun get_and_add_existing_value(testContext: VertxTestContext) = testContext.async {
        val existingValue = 1000L
        setValueAndVerifyResponse(existingValue)
        val addValue = 1L
        sut.getAndAdd(addValue).await().shouldBe(existingValue)
        sut.getAndAdd(addValue).await().shouldBe(existingValue + addValue)
    }

    @Test
    internal fun compare_and_set_no_existing_value(testContext: VertxTestContext) = testContext.async {
        sut.compareAndSet(1, 2).await().shouldBeFalse()
    }

    @Test
    internal fun compare_and_set_not_matching_value(testContext: VertxTestContext) = testContext.async {
        setValueAndVerifyResponse(1000)
        sut.compareAndSet(1, 2).await().shouldBeFalse()
    }

    @Test
    internal fun compare_and_set_matching_value(testContext: VertxTestContext) = testContext.async {
        setValueAndVerifyResponse(1)
        sut.compareAndSet(1, 2).await().shouldBeTrue()
        sut.get().await().shouldBe(2)
    }


    private suspend fun setValueAndVerifyResponse(value: Long) {
        redis.send(Request.cmd(Command.SET).arg("$redisCounterKey").arg(value)).await().isOk().shouldBeTrue()
    }
}