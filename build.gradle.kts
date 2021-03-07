import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.4.31"
    id("com.jfrog.bintray") version "1.8.5"
    `maven-publish`
}

repositories {
    jcenter()
}

dependencies {
    // BOM
    implementation(platform("io.vertx:vertx-dependencies:${Version.vertx}"))
    testImplementation(platform("org.junit:junit-bom:${Version.Test.junit}"))
    testImplementation(platform("org.testcontainers:testcontainers-bom:${Version.Test.testContainers}"))
    testImplementation(platform("org.apache.logging.log4j:log4j-bom:${Version.Test.log4j}"))

    // Prod libs
    implementation(vertx("core"))
    implementation("ch.sourcemotion.vertx.redis:vertx-redis-client-heimdall:${Version.redisHeimdall}")
    implementation("io.github.microutils:kotlin-logging:${Version.kotlinLogging}")
    implementation("de.ruedigermoeller:fst:${Version.fst}")

    // Test libs
    testImplementation(vertx("junit5"))
    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl")
    testImplementation("org.apache.logging.log4j:log4j-core")
    testImplementation("io.kotest:kotest-assertions-core-jvm:${Version.Test.kotest}")
    testImplementation("com.fasterxml.jackson.module:jackson-module-kotlin") { version { strictly(Version.jackson) } }

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

fun vertx(module: String): String = "io.vertx:vertx-$module"

tasks {
    test {
        useJUnitPlatform()
    }

    withType<KotlinCompile> {
        kotlinOptions {
            jvmTarget = Version.java
            apiVersion = "1.4"
            languageVersion = "1.4"
            freeCompilerArgs += listOf("-Xinline-classes")
        }
    }
}

object Version {
    const val java = "11"
    const val vertx = "4.0.2"
    const val jackson = "2.11.3"
    const val redisHeimdall = "1.0.0"
    const val kotlinLogging = "2.0.4"
    const val fst = "2.56"

    object Test {
        const val junit = "5.7.0"
        const val kotest = "4.4.1"
        const val testContainers = "1.15.2"
        const val log4j = "2.14.0"
    }
}