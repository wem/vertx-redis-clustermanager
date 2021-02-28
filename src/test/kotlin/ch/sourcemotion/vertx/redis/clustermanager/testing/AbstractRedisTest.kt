package ch.sourcemotion.vertx.redis.clustermanager.testing

import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdall
import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdallOptions
import ch.sourcemotion.vertx.redis.clustermanager.testing.container.TestContainer
import ch.sourcemotion.vertx.redis.clustermanager.testing.container.TestContainer.Companion.REDIS_PORT
import ch.sourcemotion.vertx.redis.clustermanager.testing.extension.SingletonContainer
import ch.sourcemotion.vertx.redis.clustermanager.testing.extension.SingletonContainerExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.RedisOptions
import io.vertx.redis.client.Request
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.testcontainers.containers.Network
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.LazyThreadSafetyMode.NONE

@ExtendWith(SingletonContainerExtension::class)
internal abstract class AbstractRedisTest : AbstractVertxTest() {

    companion object {
        val redisNetwork = Network.newNetwork()

        @JvmStatic
        @get:SingletonContainer
        val redisContainer = TestContainer.createRedisContainer(redisNetwork)
    }

    protected fun TestContainer.redisAddress() = "redis://$containerIpAddress:${getMappedPort(REDIS_PORT)}"

    protected fun getRedisHeimdallOptions() =
        RedisHeimdallOptions(RedisOptions().setConnectionString(redisContainer.redisAddress()))

    protected val redis by lazy(NONE) { RedisHeimdall.create(vertx, getRedisHeimdallOptions()) }

    @AfterEach
    internal fun flushAll(testContext: VertxTestContext) = testContext.async {
        redis.send(Request.cmd(Command.FLUSHALL)).await()
    }
}
