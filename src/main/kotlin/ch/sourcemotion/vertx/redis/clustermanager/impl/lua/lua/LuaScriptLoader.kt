package ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua

import io.vertx.core.Future
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request

/**
 * Simple a loader with cache of LUA scripts used for short, fast operations against Redis.
 */
internal object LuaScriptLoader {

    private val scriptCache = HashMap<LuaScript, String>()

    fun loadScriptSha(script: LuaScript, redis: Redis): Future<String> {
        return if (scriptCache.containsKey(script)) {
            Future.succeededFuture(scriptCache[script])
        } else {
            val scriptContent = LuaScriptLoader::class.java.getResourceAsStream(script.path)
                .use { ins -> ins.bufferedReader(Charsets.UTF_8).use { reader -> reader.readText() } }
            return loadScriptRedis(scriptContent, redis).onSuccess { scriptCache[script] = it }
        }
    }

    private fun loadScriptRedis(scriptContent: String, redis: Redis): Future<String> {
        return redis.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(scriptContent))
            .compose { Future.succeededFuture(it.toString()) }
    }
}
