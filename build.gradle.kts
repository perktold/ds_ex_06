plugins {
    kotlin("jvm") version "1.9.24"
    application
}
repositories { mavenCentral() }
dependencies {
    implementation("io.dapr:dapr-sdk:1.14.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.1")
    implementation("org.slf4j:slf4j-simple:2.0.13")
}
application { mainClass.set("MainKt") }