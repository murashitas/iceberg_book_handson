plugins {
    id("java")
}

group = "jp.gihyo.iceberg"
version = "0.1"

val icebergVersion = "1.8.1"
val junitVersion = "5.10.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.iceberg:iceberg-api:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-core:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-data:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-parquet:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-bundled-guava:$icebergVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}