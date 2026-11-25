plugins {
    kotlin("jvm") version "2.1.0"
    application
}

repositories { mavenCentral() }

dependencies {
    implementation("io.dapr:dapr-sdk:1.19.0")
    implementation("org.slf4j:slf4j-simple:2.0.16") // silences SLF4J warnings, optional
}

kotlin { jvmToolchain(21) }

application { mainClass.set("MainKt") }