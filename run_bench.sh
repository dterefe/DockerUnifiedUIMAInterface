#!/bin/bash
set -e
JH=/home/stud_homes/s0424382/.local/opt/jdk21-extracted/usr/lib/jvm/java-21-openjdk-amd64
MCP=$(JAVA_HOME=$JH mvn dependency:build-classpath -pl duui-dua/dua-core -o -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout 2>/dev/null)
MCP=$(echo "$MCP" | tr ":" "\n" | grep -v "duui-core" | grep -v "duui-base" | grep -v "UIMATypeSystem" | grep -v "annotation/typesystem" | grep -v "Utilities" | tr "\n" ":")
OUT=duui-dua/dua-core/target/classes
TEST=duui-dua/dua-core/target/test-classes
$JH/bin/javac --enable-preview --release 21 -proc:none -cp "$OUT:$MCP" -d "$OUT" duui-dua/dua-core/src/main/java/org/apache/uima/cas/impl/FfmFeatureStructureBackend.java
$JH/bin/javac --enable-preview --release 21 -proc:none -cp "$OUT:$TEST:$MCP" -d "$TEST" duui-dua/dua-core/src/test/java/org/apache/uima/cas/impl/benchmark/JcasBackendBenchmark.java
$JH/bin/java --enable-preview --enable-native-access=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED -Xmx24g -cp "$OUT:$TEST:$MCP" org.apache.uima.cas.impl.benchmark.JcasBackendBenchmark --count 100
