package ch.sourcemotion.vertx.redis.clustermanager.testing.container

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

internal class TestContainer(dockerImageName: DockerImageName) : GenericContainer<TestContainer>(dockerImageName) {
    companion object {
        const val REDIS_PORT = 6379
        private val redisImageName = DockerImageName.parse("redis:5.0.11-alpine")
        fun createRedisContainer(network: Network? = null): TestContainer =
            TestContainer(redisImageName).withExposedPorts(REDIS_PORT).also {
                if (network != null) {
                    it.withNetwork(network)
                }
            }
    }
}
