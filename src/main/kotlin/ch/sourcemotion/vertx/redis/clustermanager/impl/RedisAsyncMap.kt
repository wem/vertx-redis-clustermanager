package ch.sourcemotion.vertx.redis.clustermanager.impl

import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaExecutor
import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaScript
import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.toBytes
import ch.sourcemotion.vertx.redis.clustermanager.spi.ClusterMapSerialization
import io.vertx.core.Future
import io.vertx.core.shareddata.AsyncMap
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request
import io.vertx.redis.client.Response

internal class RedisAsyncMap<K : Any, V : Any>(
    clusterName: ClusterName,
    mapName: RedisMapName,
    serialization: ClusterMapSerialization<K, V>,
    private val redis: Redis,
    private val luaExecutor: LuaExecutor
) : RedisMapBase<K, V>(mapName), AsyncMap<K, V> {

    private companion object {
        val removeOrIfPresentScript = LuaScript("/lua/asyncmap/remove_or_if_present.lua")
        val putIfAbsentScript = LuaScript("/lua/asyncmap/put_if_absent.lua")
        val replaceScript = LuaScript("/lua/asyncmap/replace.lua")
        val replaceIfPresentScript = LuaScript("/lua/asyncmap/replace_if_present.lua")
        val clearScript = LuaScript("/lua/asyncmap/clear.lua")
        val sizeScript = LuaScript("/lua/asyncmap/size.lua")
        val valuesScript = LuaScript("/lua/asyncmap/values.lua")
        val entriesScript = LuaScript("/lua/asyncmap/entries.lua")
    }

    private val mapKey = clusterName.serviceKey(mapName)
    private val mapKeyWildCard = mapKey + "*".toByteArray()
    private val mapKeySize = mapKey.size
    private val keySerialization = serialization.keySerialization
    private val valueSerialization = serialization.valueSerialization

    override fun get(k: K) = mapCallWith(k) { entryKey ->
        redis.send(Request.cmd(Command.GET).arg(entryKey)).compose {
            val value = if (it != null) {
                valueSerialization.deserialize(it.toBytes())
            } else null
            Future.succeededFuture(value)
        }
    }

    override fun put(k: K, v: V): Future<Void> = mapCallWith(k, v) { entryKey, entryValue ->
        return redis.send(Request.cmd(Command.SET).arg(entryKey).arg(entryValue))
            .compose { Future.succeededFuture(null) }
    }

    override fun put(k: K, v: V, ttl: Long): Future<Void> = mapCallWith(k, v) { entryKey, entryValue ->
        return redis.send(Request.cmd(Command.SET).arg(entryKey).arg(entryValue).arg("PX").arg("$ttl"))
            .compose { Future.succeededFuture(null) }
    }

    override fun putIfAbsent(k: K, v: V): Future<V?> = mapCallWith(k, v) { entryKey, entryValue ->
        return luaExecutor.execute(putIfAbsentScript, listOf(entryKey), listOf(entryValue)).compose {
            val responseValue = if (it != null) {
                valueSerialization.deserialize(it.toBytes())
            } else null
            Future.succeededFuture(responseValue)
        }
    }

    override fun putIfAbsent(k: K, v: V, ttl: Long): Future<V?> = mapCallWith(k, v) { entryKey, entryValue ->
        return luaExecutor.execute(putIfAbsentScript, listOf(entryKey), listOf(entryValue, ttl.toBytes())).compose {
            val responseValue = if (it != null) {
                valueSerialization.deserialize(it.toBytes())
            } else null
            Future.succeededFuture(responseValue)
        }
    }

    override fun remove(k: K): Future<V?> = mapCallWith(k) { entryKey ->
        return luaExecutor.execute(removeOrIfPresentScript, listOf(entryKey)).compose { it.deserializeAsValue(k) }
    }

    override fun removeIfPresent(k: K, v: V): Future<Boolean> = mapCallWith(k, v) { entryKey, entryValue ->
        return luaExecutor.execute(removeOrIfPresentScript, listOf(entryKey), listOf(entryValue))
            .compose { Future.succeededFuture(it.toBoolean()) }
    }

    override fun replace(k: K, v: V): Future<V?> = mapCallWith(k, v) { entryKey, entryValue ->
        return luaExecutor.execute(replaceScript, listOf(entryKey), listOf(entryValue)).compose {
            val responseValue = if (it != null) {
                valueSerialization.deserialize(it.toBytes())
            } else null
            Future.succeededFuture(responseValue)
        }
    }

    override fun replaceIfPresent(k: K, oldValue: V, newValue: V): Future<Boolean> =
        mapCallWith(k, oldValue) { entryKey, oldEntryValue ->
            val newEntryValue = valueSerialization.runCatching { serialize(newValue) }
                .getOrElse { return Future.failedFuture(serializeValueExceptionOf(it, k, newValue)) }

            return luaExecutor.execute(replaceIfPresentScript, listOf(entryKey), listOf(oldEntryValue, newEntryValue))
                .compose { Future.succeededFuture(it.toBoolean()) }
        }

    private inline fun <T> mapCallWith(k: K, block: (ByteArray) -> Future<T>): Future<T> {
        val entryKey = keySerialization.runCatching { entryKeyOf(k) }
            .getOrElse { return Future.failedFuture(serializeKeyExceptionOf(it, k)) }
        return block(entryKey)
    }

    private inline fun <T> mapCallWith(k: K, v: V, block: (ByteArray, ByteArray) -> Future<T>): Future<T> {
        val entryKey = keySerialization.runCatching { entryKeyOf(k) }
            .getOrElse { return Future.failedFuture(serializeKeyExceptionOf(it, k)) }
        val entryValue = valueSerialization.runCatching { serialize(v) }
            .getOrElse { return Future.failedFuture(serializeValueExceptionOf(it, k, v)) }
        return block(entryKey, entryValue)
    }

    override fun clear(): Future<Void> {
        return luaExecutor.execute(clearScript, listOf(mapKeyWildCard)).compose {
            Future.succeededFuture(null)
        }
    }

    override fun size(): Future<Int> {
        return luaExecutor.execute(sizeScript, listOf(mapKeyWildCard))
            .compose { Future.succeededFuture(it.toInteger()) }
    }

    override fun keys(): Future<Set<K>> {
        return redis.send(Request.cmd(Command.KEYS).arg(mapKeyWildCard)).compose {
            val responseValue = it.map { keyEntry ->
                val keyEntryBytes = keyEntry.toBytes()
                keyEntryBytes.copyOfRange(mapKeySize, keyEntryBytes.size)
            }.map { keyValue -> keySerialization.deserialize(keyValue) }.toSet()
            Future.succeededFuture(responseValue)
        }
    }

    override fun values(): Future<List<V>> {
        return luaExecutor.execute(valuesScript, listOf(mapKeyWildCard)).compose {
            val values = it.map { valueEntry -> valueSerialization.deserialize(valueEntry.toBytes()) }
            Future.succeededFuture(values)
        }
    }

    override fun entries(): Future<Map<K, V>> {
        return luaExecutor.execute(entriesScript, listOf(mapKeyWildCard)).compose {
            val entries = HashMap<K, V>()
            val keys = ArrayList<K>()
            val values = ArrayList<V>()
            it.forEachIndexed { index, entryKeyOrValue ->
                val keyOrValueBytes = entryKeyOrValue.toBytes()
                if (index % 2 == 0) {
                    keys.add(
                        keySerialization.deserialize(keyOrValueBytes.copyOfRange(mapKeySize, keyOrValueBytes.size))
                    )
                } else {
                    values.add(valueSerialization.deserialize(keyOrValueBytes))
                }
            }
            keys.forEachIndexed { index, key ->
                entries[key] = values[index]
            }
            Future.succeededFuture(entries)
        }
    }

    private fun Response?.deserializeAsValue(k: K): Future<V?> = if (this != null) {
        val rawValue = toBytes()
        val value = valueSerialization.runCatching { deserialize(rawValue) }.getOrElse {
            return Future.failedFuture(deserializeValueExceptionOf(it, k, rawValue))
        }
        Future.succeededFuture(value)
    } else Future.succeededFuture()

    private fun entryKeyOf(k: K) = mapKey + keySerialization.serialize(k)
}