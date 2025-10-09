#!/bin/bash

# Script to compile Delta Sharing project with Java 17

# Configure Java 17
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"
export PATH="$JAVA_HOME/bin:$PATH"

echo "🔨 Compiling Delta Sharing OnPrem..."
echo "📌 Java Version:"
java -version

echo ""
echo "🔧 Compiling..."
mvn clean package
