package ch.sourcemotion.vertx.redis.clustermanager.spi

interface ClusterSerialization<T> {
    fun serialize(value: T) : ByteArray
    fun deserialize(value: ByteArray) : T
}