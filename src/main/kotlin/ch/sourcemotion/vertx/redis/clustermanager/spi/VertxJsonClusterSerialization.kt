package ch.sourcemotion.vertx.redis.clustermanager.spi

import io.vertx.core.json.JsonObject

class VertxJsonClusterSerialization<T>(private val type: Class<T>) : ClusterSerialization<T> {

    companion object {
        inline fun <reified T : Any> of() = VertxJsonClusterSerialization(T::class.java)
    }

    override fun serialize(value: T): ByteArray = JsonObject.mapFrom(value).encode().toByteArray(Charsets.UTF_8)
    override fun deserialize(value: ByteArray): T = JsonObject(value.toString(Charsets.UTF_8)).mapTo(type)
}