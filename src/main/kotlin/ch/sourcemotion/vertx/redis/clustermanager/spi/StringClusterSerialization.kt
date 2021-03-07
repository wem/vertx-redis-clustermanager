package ch.sourcemotion.vertx.redis.clustermanager.spi

object StringClusterSerialization : ClusterSerialization<String> {
    override fun serialize(value: String) = value.toByteArray(Charsets.UTF_8)
    override fun deserialize(value: ByteArray) = value.toString(Charsets.UTF_8)
}