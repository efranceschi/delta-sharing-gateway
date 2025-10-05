# 🚀 Quick Start - Delta Sharing OnPrem

## ✅ Issue Resolved

The project had an error with **duplicate OpenAPI beans**. This was fixed by removing the duplicate configuration.

## 📋 Prerequisites

- ✅ Java 17 (installed via Homebrew)
- ✅ Maven 3.6+
- ✅ Lombok configured

## 🏃 How to Run

### 1. Compile the Project

```bash
./compile.sh
```

**Expected output:**
```
[INFO] BUILD SUCCESS
[INFO] Total time: ~2-3 seconds
```

### 2. Run the Application

```bash
./run.sh
```

**Expected output:**
```
🚀 Starting Delta Sharing OnPrem...
📌 Java Version:
openjdk version "17.0.16" 2025-07-15

Started DeltaSharingApplication in X.XXX seconds
```

The application will be available at: **http://localhost:8080**

## 🧪 Test the Application

### 1. Check if it's running

```bash
curl http://localhost:8080/
```

### 2. Test Delta Sharing API

```bash
# Set token
export TOKEN="test-token"

# List shares
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares | jq

# List schemas
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas | jq

# List tables
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas/default/tables | jq
```

### 3. Access Web Interfaces

- **Home Page**: http://localhost:8080/
- **Swagger UI**: http://localhost:8080/swagger-ui.html
- **H2 Console**: http://localhost:8080/h2-console
  - JDBC URL: `jdbc:h2:mem:deltasharing`
  - Username: `sa`
  - Password: (empty)

## 📊 Sample Data

The server comes pre-loaded with sample data:

### Shares
- `demo-share` - Demo share
- `sales-share` - Sales share

### Schemas
- `default` - Default schema
- `analytics` - Analytics schema
- `sales` - Sales schema

### Tables
- `customers` - Customer data
- `orders` - Orders
- `revenue` - Revenue
- `transactions` - Transactions

## 🔧 Troubleshooting

### Error: HTTP 403 Forbidden on Delta Sharing endpoints

**Cause**: Authentication was not being created in SecurityContext

**Solution**: ✅ Already fixed! The filter now creates the Authentication object.

### Error: "Cannot find symbol" or methods not found

**Cause**: Lombok is not processing annotations

**Solution**: Make sure you're using Java 17
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
java -version  # Should show 17.x.x
```

### Error: "2 beans found" for OpenAPI

**Cause**: Duplicate OpenAPI configuration

**Solution**: ✅ Already fixed! The `OpenApiConfig.java` file was removed.

### Error: UnsupportedClassVersionError (class file version 61.0)

**Cause**: Trying to run JAR compiled with Java 17 using Java 8

**Solution**: Use the provided scripts that automatically configure Java 17
```bash
./run.sh  # Always use this script
```

### Port 8080 already in use

**Solution**: Kill the process or use another port
```bash
# Kill process on port 8080
lsof -ti:8080 | xargs kill -9

# Or configure another port in application.yml
server:
  port: 8081
```

## 📝 Useful Commands

```bash
# Compile
./compile.sh

# Run
./run.sh

# Compile and generate JAR
mvn clean package

# Run JAR directly
java -jar target/delta-sharing-onprem-1.0.0.jar

# View logs in real-time (if running in background)
tail -f logs/spring.log

# Stop the application
# Ctrl+C (if running in foreground)
# or
pkill -f delta-sharing-onprem
```

## 🎯 Next Steps

1. **Explore the API**
   - Access http://localhost:8080/swagger-ui.html
   - Test the Delta Sharing endpoints

2. **Read the Complete Documentation**
   - [DELTA_SHARING_README.md](DELTA_SHARING_README.md) - Protocol documentation
   - [TESTING.md](TESTING.md) - Testing guide
   - [README.md](README.md) - General documentation

3. **Test with Python Client**
   ```bash
   pip install delta-sharing
   # Create config.share and test
   ```

## ✅ Verification Checklist

- [x] Java 17 installed and configured
- [x] Project compiles without errors
- [x] Application starts correctly
- [x] Delta Sharing endpoints respond
- [x] Swagger UI accessible
- [x] Sample data loaded

## 🆘 Support

If you encounter problems:

1. Check the application logs
2. Confirm you're using Java 17: `java -version`
3. Try recompiling: `./compile.sh`
4. Consult [TESTING.md](TESTING.md) for test examples

---

**Status**: ✅ Project 100% functional and ready to use!
