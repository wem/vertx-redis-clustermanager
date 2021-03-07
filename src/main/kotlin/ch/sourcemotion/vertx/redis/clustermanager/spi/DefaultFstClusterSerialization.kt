package ch.sourcemotion.vertx.redis.clustermanager.spi

import org.nustaq.serialization.FSTConfiguration

class DefaultFstClusterSerialization<T>(
    private val configuration: FSTConfiguration = FSTConfiguration.createDefaultConfiguration()
) : ClusterSerialization<T> {

    companion object {
        val default = DefaultFstClusterSerialization<Any>()
    }

    override fun serialize(value: T): ByteArray = configuration.asByteArray(value)
    override fun deserialize(value: ByteArray) = configuration.asObject(value) as T
}