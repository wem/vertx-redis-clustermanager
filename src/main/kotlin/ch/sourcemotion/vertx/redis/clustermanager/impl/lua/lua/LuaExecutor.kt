package ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua

import io.vertx.core.Future
import io.vertx.redis.client.Command
import io.vertx.redis.client.Redis
import io.vertx.redis.client.Request
import io.vertx.redis.client.Response
import io.vertx.redis.client.impl.RESPEncoder

internal class LuaExecutor(private val redis: Redis) {
    fun execute(
        script: LuaScript,
        keys: List<ByteArray> = emptyList(),
        args: List<ByteArray> = emptyList()
    ): Future<Response> {
        return LuaScriptLoader.loadScriptSha(script, redis).compose {
            val scriptArgs = ArrayList(keys).apply { addAll(args) }
            val command = Request.cmd(Command.EVALSHA).arg(it).arg("${keys.size}")
            scriptArgs.forEach { scriptArg -> command.arg(scriptArg) }
            redis.send(command)
        }
    }
}

fun Int.toBytes(): ByteArray = RESPEncoder.numToBytes(toLong())
fun Long.toBytes(): ByteArray = RESPEncoder.numToBytes(toLong())
