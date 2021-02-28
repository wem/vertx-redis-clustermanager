package ch.sourcemotion.vertx.redis.clustermanager.impl

import io.vertx.redis.client.Response

fun Response?.isOk() = this?.toString() == "OK"