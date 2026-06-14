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

    // Apache Arrow — optional; needed only for QueryBuilder.toArrow().
    // Declare as compileOnly so callers that never use Arrow do not pull in the
    // ~10 MB transitive closure.  Callers that do use toArrow() must add
    //   runtimeOnly("org.apache.arrow:arrow-vector:19.0.0")
    // (plus arrow-memory-netty or arrow-memory-unsafe for the allocator) to
    // their own build file.
    compileOnly("org.apache.arrow:arrow-vector:19.0.0")
    compileOnly("org.apache.arrow:arrow-memory-core:19.0.0")

    // JUnit 5 test suite
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // Arrow on the test classpath so Arrow tests compile and run without a
    // separate caller build file.
    testRuntimeOnly("org.apache.arrow:arrow-memory-unsafe:19.0.0")
    testImplementation("org.apache.arrow:arrow-vector:19.0.0")
    testImplementation("org.apache.arrow:arrow-memory-core:19.0.0")
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
