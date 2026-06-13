plugins {
    `java-library`
    `maven-publish`
}

group = "io.basin"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
    withJavadocJar()
    withSourcesJar()
}

repositories {
    mavenCentral()
}

dependencies {
    // Jackson for JSON — the only runtime dep for the core SDK.
    // Uses the standard ObjectMapper; callers may supply their own via BasinClient.Builder.
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")

    // JUnit 5 test suite
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("Basin Java SDK")
                description.set("JVM client for Basin: auth, query, storage, realtime, functions.")
                url.set("https://github.com/bas-in/basin")
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
            }
        }
    }
}
