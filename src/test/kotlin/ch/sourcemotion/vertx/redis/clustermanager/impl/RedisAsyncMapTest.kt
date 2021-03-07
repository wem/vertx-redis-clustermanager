package ch.sourcemotion.vertx.redis.clustermanager.impl

import ch.sourcemotion.vertx.redis.clustermanager.impl.lua.lua.LuaExecutor
import ch.sourcemotion.vertx.redis.clustermanager.spi.ClusterMapSerialization
import ch.sourcemotion.vertx.redis.clustermanager.testing.AbstractRedisTest
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.maps.shouldContainAll
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test
import java.io.Serializable
import kotlin.LazyThreadSafetyMode.NONE

data class MapKey(val keyValue: String) : Serializable
data class MapValue(val value: String) : Serializable

internal class RedisAsyncMapTest : AbstractRedisTest() {

    private companion object {
        val clusterName = ClusterName("cluster")
        val mapName = RedisMapName("map")
    }

    private val sut by lazy(NONE) { RedisAsyncMap<MapKey, MapValue>(clusterName, mapName, ClusterMapSerialization.default(), redis, LuaExecutor(redis)) }

    @Test
    internal fun put_get(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val expectValue = MapValue("value")

        sut.put(key, expectValue).await()
        sut.get(key).await().shouldBe(expectValue)
        sut.get(MapKey("not-existing")).await().shouldBeNull()
    }

    @Test
    internal fun put_if_absent(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val initialValue = MapValue("value")
        val updateValue = MapValue("update-value")

        sut.putIfAbsent(key, initialValue).await().shouldBeNull()
        sut.putIfAbsent(key, updateValue).await().shouldBe(initialValue)
    }

    @Test
    internal fun put_if_absent_ttl(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val value = MapValue("value")
        val ttl = 20L

        sut.putIfAbsent(key, value, ttl).await().shouldBeNull()
        delay(ttl * 2)
        sut.putIfAbsent(key, value, ttl).await().shouldBeNull()
    }

    @Test
    internal fun remove_existing_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val value = MapValue("value")

        sut.put(key, value).await()
        sut.remove(key).await().shouldBe(value)
    }

    @Test
    internal fun remove_no_existing_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        sut.remove(key).await().shouldBeNull()
    }

    @Test
    internal fun remove_if_present_matching_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val matchingValue = MapValue("value")

        sut.put(key, matchingValue).await()
        sut.removeIfPresent(key, matchingValue).await().shouldBeTrue()
    }

    @Test
    internal fun remove_if_present_no_matching_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val value = MapValue("value")
        val notMatchingValue = MapValue("anotherValue")

        sut.put(key, value).await()
        sut.removeIfPresent(key, notMatchingValue).await().shouldBeFalse()
    }

    @Test
    internal fun remove_if_present_no_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val value = MapValue("value")

        sut.removeIfPresent(key, value).await().shouldBeFalse()
    }

    @Test
    internal fun replace_existing_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val previousValue = MapValue("previous-value")
        val newValue = MapValue("new-value")

        sut.put(key, previousValue).await()
        sut.replace(key, newValue).await().shouldBe(previousValue)
        sut.get(key).await().shouldBe(newValue)
    }

    @Test
    internal fun replace_not_existing_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val value = MapValue("value")

        sut.replace(key, value).await().shouldBeNull()
        sut.get(key).await().shouldBeNull()
    }

    @Test
    internal fun replace_if_present_matching_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val oldValue = MapValue("previous-value")
        val newValue = MapValue("new-value")

        sut.put(key, oldValue).await()
        sut.replaceIfPresent(key, oldValue, newValue).await().shouldBeTrue()
    }

    @Test
    internal fun replace_if_present_no_matching_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val notMatchingValue = MapValue("not-matching-value")
        val oldValue = MapValue("previous-value")
        val newValue = MapValue("new-value")

        sut.put(key, notMatchingValue).await()
        sut.replaceIfPresent(key, oldValue, newValue).await().shouldBeFalse()
    }

    @Test
    internal fun replace_if_present_no_entry(testContext: VertxTestContext) = testContext.async {
        val key = MapKey("key")
        val oldValue = MapValue("previous-value")
        val newValue = MapValue("new-value")

        sut.replaceIfPresent(key, oldValue, newValue).await().shouldBeFalse()
    }

    @Test
    internal fun clear(testContext: VertxTestContext) = testContext.async {
        val entries = 100
        fillMap(entries)
        sut.clear().await()

        repeat(entries) { idx ->
            sut.get(mapKeyOf(idx)).await().shouldBeNull()
        }
    }

    @Test
    internal fun size_0(testContext: VertxTestContext) = testContext.async {
        sut.size().await().shouldBe(0)
    }

    @Test
    internal fun size_13(testContext: VertxTestContext) = testContext.async {
        val expectedEntryCount = 13
        fillMap(expectedEntryCount)
        sut.size().await().shouldBe(expectedEntryCount)
    }

    @Test
    internal fun size_26(testContext: VertxTestContext) = testContext.async {
        val expectedEntryCount = 26
        fillMap(expectedEntryCount)
        sut.size().await().shouldBe(expectedEntryCount)
    }

    @Test
    internal fun size_260(testContext: VertxTestContext) = testContext.async {
        val expectedEntryCount = 260
        fillMap(expectedEntryCount)
        sut.size().await().shouldBe(expectedEntryCount)
    }

    @Test
    internal fun size_2600(testContext: VertxTestContext) = testContext.async {
        val expectedEntryCount = 2600
        fillMap(expectedEntryCount)
        sut.size().await().shouldBe(expectedEntryCount)
    }

    @Test
    internal fun keys_no_entry(testContext: VertxTestContext) = testContext.async {
        sut.keys().await().shouldBeEmpty()
    }

    @Test
    internal fun keys_with_entries(testContext: VertxTestContext) = testContext.async {
        val expectedEntryCount = 9
        fillMap(expectedEntryCount)
        sut.keys().await().toList().sortedBy { it.keyValue }.forEachIndexed { idx, expectedKey ->
            mapKeyOf(idx).shouldBe(expectedKey)
        }
    }

    @Test
    internal fun values_no_entry(testContext: VertxTestContext) = testContext.async {
        sut.values().await().shouldBeEmpty()
    }

    @Test
    internal fun values_with_entries(testContext: VertxTestContext) = testContext.async {
        val expectedEntryCount = 9
        fillMap(expectedEntryCount)
        sut.values().await().toList().sortedBy { it.value }.forEachIndexed { idx, expectedValue ->
            mapValueOf(idx).shouldBe(expectedValue)
        }
    }

    @Test
    internal fun entries_no_entry(testContext: VertxTestContext) = testContext.async {
        sut.entries().await().shouldBeEmpty()
    }

    @Test
    internal fun entries(testContext: VertxTestContext) = testContext.async {
        val expectedEntryCount = 9
        fillMap(expectedEntryCount)
        val entries = sut.entries().await()
        repeat(expectedEntryCount) { idx ->
            entries.shouldContain(Pair(mapKeyOf(idx), mapValueOf(idx)))
        }
    }

    private suspend fun fillMap(entries: Int) {
        repeat(entries) { idx ->
            sut.put(mapKeyOf(idx), mapValueOf(idx)).await()
        }
    }

    private fun mapKeyOf(nbr: Int) = MapKey("key-$nbr")
    private fun mapValueOf(nbr: Int) = MapValue("value-$nbr")
}