package ch.sourcemotion.vertx.redis.clustermanager.testing.extension

import mu.KLogging
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.platform.commons.util.AnnotationUtils
import org.junit.platform.commons.util.ReflectionUtils
import org.testcontainers.containers.GenericContainer
import org.testcontainers.shaded.com.trilead.ssh2.log.Logger.logger

class SingletonContainerExtension : BeforeAllCallback {

    private companion object : KLogging()

    override fun beforeAll(context: ExtensionContext) {
        val containers = findContainers(context)
        containers.forEach { container ->
            runCatching { container.start() }.onFailure {
                logger.error(it) { "Unable to start Docker container of image \"${container.dockerImageName}\"" }
                throw it
            }.onSuccess { logger.info { "---- ${container.dockerImageName} started ----" } }
        }
    }

    private fun findContainers(context: ExtensionContext): List<GenericContainer<*>> {
        val testClass = context.testClass
        return if (testClass.isPresent) {
            ReflectionUtils.findMethods(
                testClass.get(),
                {
                    AnnotationUtils.isAnnotated(it,
                        SingletonContainer::class.java
                    ) && ReflectionUtils.isStatic(it)
                },
                ReflectionUtils.HierarchyTraversalMode.TOP_DOWN
            ).map { it.invoke(null) }.filterIsInstance<GenericContainer<*>>()
        } else {
            emptyList()
        }
    }
}


@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.PROPERTY_GETTER)
annotation class SingletonContainer
