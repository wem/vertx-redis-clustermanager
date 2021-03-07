package ch.sourcemotion.vertx.redis.clustermanager.spi

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import java.io.Serializable

internal class DefaultFstClusterSerializationTest {
    private val sut = DefaultFstClusterSerialization<Any>()

    @Test
    internal fun primitives_serialization() {
        sut.deserialize(sut.serialize(1000)).shouldBe(1000)
        sut.deserialize(sut.serialize(1000L)).shouldBe(1000L)
        sut.deserialize(sut.serialize("some-string")).shouldBe("some-string")
        sut.deserialize(sut.serialize("√")).shouldBe("√")
    }

    @Test
    internal fun data_class_serialization() {
        val expected = SerializableDataClass()
        sut.deserialize(sut.serialize(expected)).shouldBe(expected)
    }

    @Test
    internal fun common_class_serialization() {
        val expected = CommonClass()
        sut.deserialize(sut.serialize(expected)).shouldBe(expected)
    }
}

data class SerializableDataClass(val member: Int = 100) : Serializable

class CommonClass(val member: Int = 100) : Serializable {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is CommonClass) return false

        if (member != other.member) return false

        return true
    }

    override fun hashCode(): Int {
        return member
    }
}

