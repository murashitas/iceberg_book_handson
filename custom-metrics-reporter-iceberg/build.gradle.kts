plugins {
    id("java")
    id("com.diffplug.spotless") version "6.25.0"
}

group = "jp.gihyo.iceberg"
version = "0.1"

val icebergVersion = "1.8.1"
val sparkVersion = "3.5.2"
val junitVersion = "5.10.0"

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.apache.iceberg:iceberg-api:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-core:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-data:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-parquet:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-bundled-guava:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$icebergVersion")
    
    compileOnly("org.apache.spark:spark-sql_2.12:$sparkVersion")
    compileOnly("org.apache.spark:spark-core_2.12:$sparkVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}