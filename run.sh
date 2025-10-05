#!/bin/bash

# Script to run Delta Sharing project with Java 17

# Configure Java 17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
export DELTA_SHARING_TOKEN=test

echo "🚀 Starting Delta Sharing OnPrem..."
echo "📌 Java Version:"
java -version

echo ""
echo "🔧 Compiling and running..."
mvn spring-boot:run
