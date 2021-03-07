package ch.sourcemotion.vertx.redis.clustermanager.impl

import ch.sourcemotion.vertx.redis.client.heimdall.RedisHeimdall
import ch.sourcemotion.vertx.redis.clustermanager.RedisClusterManager
import ch.sourcemotion.vertx.redis.clustermanager.RedisClusterManagerOptions
import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaExecutor
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.shareddata.AsyncMap
import io.vertx.core.shareddata.Counter
import io.vertx.core.shareddata.Lock
import io.vertx.core.spi.cluster.NodeInfo
import io.vertx.core.spi.cluster.NodeListener
import io.vertx.core.spi.cluster.NodeSelector
import io.vertx.core.spi.cluster.RegistrationInfo
import io.vertx.redis.client.Redis

interface ServiceName {
    val name: String
}

internal class ClusterName(private val value: String, val hashKey: ByteArray = "{$value}".toByteArray(Charsets.UTF_8)) {
    fun serviceKey(serviceName: ServiceName) = hashKey + serviceName.name.toByteArray()
    override fun toString() = value
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ClusterName) return false

        if (value != other.value) return false
        if (!hashKey.contentEquals(other.hashKey)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = value.hashCode()
        result = 31 * result + hashKey.contentHashCode()
        return result
    }
}

internal class RedisClusterManagerImpl(
    private val options: RedisClusterManagerOptions
) : RedisClusterManager {

    private val clusterName = ClusterName(options.clusterName)
    private lateinit var luaExecutor: LuaExecutor
    private lateinit var redis: Redis
    private lateinit var vertx: Vertx
    private lateinit var nodeSelector: NodeSelector
    private lateinit var lockFactory: RedisLockFactory

    override fun init(vertx: Vertx, nodeSelector: NodeSelector) {
        this.vertx = vertx
        this.nodeSelector = nodeSelector
        this.redis = RedisHeimdall.create(vertx, options.redisOptions)
        this.luaExecutor = LuaExecutor(redis)
        lockFactory = RedisLockFactory(
            vertx,
            clusterName,
            redis,
            luaExecutor,
            options.lockAcquisitionRetryIntervalMillis,
            options.lockExpirationMillis
        )
    }

    override fun <K : Any, V : Any> getAsyncMap(name: String, promise: Promise<AsyncMap<K, V>>) {
        TODO("Not yet implemented")
    }

    override fun <K : Any, V : Any> getSyncMap(name: String): MutableMap<K, V> {
        TODO("Not yet implemented")
    }

    override fun getLockWithTimeout(name: String, timeout: Long, promise: Promise<Lock>) {
        lockFactory.createLock(RedisLockName("$clusterName-$name"), timeout, promise)
    }

    override fun getCounter(name: String, promise: Promise<Counter>) {
        promise.complete(RedisCounterImpl(clusterName, RedisCounterName(name), redis, luaExecutor))
    }

    override fun getNodeId(): String {
        TODO("Not yet implemented")
    }

    override fun getNodes(): MutableList<String> {
        TODO("Not yet implemented")
    }

    override fun nodeListener(listener: NodeListener) {
        TODO("Not yet implemented")
    }

    override fun setNodeInfo(nodeInfo: NodeInfo, promise: Promise<Void>) {
        TODO("Not yet implemented")
    }

    override fun getNodeInfo(): NodeInfo {
        TODO("Not yet implemented")
    }

    override fun getNodeInfo(nodeId: String, promise: Promise<NodeInfo>) {
        TODO("Not yet implemented")
    }

    override fun join(promise: Promise<Void>) {
        TODO("Not yet implemented")
    }

    override fun leave(promise: Promise<Void>) {
        TODO("Not yet implemented")
    }

    override fun isActive(): Boolean {
        TODO("Not yet implemented")
    }

    override fun addRegistration(address: String, registrationInfo: RegistrationInfo, promise: Promise<Void>) {
        TODO("Not yet implemented")
    }

    override fun removeRegistration(address: String, registrationInfo: RegistrationInfo, promise: Promise<Void>) {
        TODO("Not yet implemented")
    }

    override fun getRegistrations(address: String, promise: Promise<MutableList<RegistrationInfo>>) {
        TODO("Not yet implemented")
    }
}