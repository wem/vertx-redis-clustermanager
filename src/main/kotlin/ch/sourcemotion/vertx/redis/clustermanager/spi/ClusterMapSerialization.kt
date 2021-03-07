package ch.sourcemotion.vertx.redis.clustermanager.spi

data class ClusterMapSerialization<K, V>(
    val keySerialization: ClusterSerialization<K>,
    val valueSerialization: ClusterSerialization<V>
) {
    companion object {
        val default =
            ClusterMapSerialization(DefaultFstClusterSerialization.default, DefaultFstClusterSerialization.default)

        fun <K : Any, V : Any> default() = default as ClusterMapSerialization<K, V>
    }
}