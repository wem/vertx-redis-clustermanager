package ch.sourcemotion.vertx.redis.clustermanager.impl

import ch.sourcemotion.vertx.redis.clustermanager.RedisClusterManagerException

internal inline class RedisMapName(override val name: String) : ServiceName {
    override fun toString() = name
}

internal abstract class RedisMapBase<K: Any, V: Any>(private val mapName: RedisMapName) {
    protected fun serializeKeyExceptionOf(cause: Throwable, k: K) = RedisClusterManagerException("Failed to serialize key \"$k\" for map \"$mapName\"", cause)
    protected fun serializeValueExceptionOf(cause: Throwable, k: K, v: V) = RedisClusterManagerException("Failed to serialize value \"$v\" for map \"$mapName\" on key \"$k\"", cause)
    protected fun deserializeKeyExceptionOf(cause: Throwable, k: ByteArray) = RedisClusterManagerException("Failed to deserialize key \"$k\" for map \"$mapName\"", cause)
    protected fun deserializeValueExceptionOf(cause: Throwable, k: K, v: ByteArray) = RedisClusterManagerException("Failed to deserialize value \"$v\" for map \"$mapName\" on key \"$k\"", cause)
}