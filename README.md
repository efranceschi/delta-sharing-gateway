# Delta Sharing OnPrem

Modern Web Application with **Delta Sharing Protocol** REST API built using Spring Boot 3.2 and Java 17.

> **🎯 Delta Sharing Protocol Implementation**: This project implements a complete Delta Sharing server in 100% Java, compatible with the official [Delta Sharing Protocol specification](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md).
>
> **📖 For detailed Delta Sharing documentation, see [DELTA_SHARING_README.md](DELTA_SHARING_README.md)**

## 🚀 Features

### Delta Sharing Protocol (Primary Feature)
- **✅ Full Delta Sharing Protocol Implementation**: 100% Java implementation compatible with the official specification
- **🔌 All Protocol Endpoints**: `/shares`, `/schemas`, `/tables`, `/metadata`, `/query`, `/changes`
- **🔐 Bearer Token Authentication**: Secure authentication following Delta Sharing spec
- **📊 NDJSON Format Support**: Proper newline-delimited JSON responses for metadata and files
- **🔄 Protocol Compatibility**: Works with Python delta-sharing client, Apache Spark, and Pandas
- **📖 Complete Documentation**: See [DELTA_SHARING_README.md](DELTA_SHARING_README.md) for details

### Web Application Features
- **Modern Web UI**: Beautiful and responsive interface built with Thymeleaf and modern CSS
- **🌳 Dynamic TreeView**: Interactive tree navigation in sidebar with expand/collapse, real-time updates, and detail panels for 3-level hierarchy (Shares → Schemas → Tables)
- **📊 CRUD Management**: Complete management interface for all entities with modern card-based design
- **Management API**: Complete RESTful API with full CRUD operations for managing shares
- **Database**: JPA/Hibernate with H2 in-memory database (development) and PostgreSQL support (production)
- **API Documentation**: Interactive API documentation with Swagger UI (SpringDoc OpenAPI)
- **Validation**: Input validation with Bean Validation
- **Exception Handling**: Global exception handling with custom error responses
- **Hot Reload**: Spring Boot DevTools for faster development

## 📋 Prerequisites

- Java 17 or higher
- Maven 3.6 or higher

## 🛠️ Technology Stack

- **Spring Boot 3.2.0**
- **Java 17**
- **Thymeleaf** - Template engine for web pages
- **Spring Data JPA** - Database access
- **H2 Database** - In-memory database for development
- **PostgreSQL** - Production database support
- **SpringDoc OpenAPI** - API documentation
- **Lombok** - Reduce boilerplate code
- **Maven** - Dependency management

## 🏃 Running the Application

### ⚠️ Importante: Java 17 Requerido

Este projeto requer **Java 17**. O sistema possui Java 8 por padrão, então use os scripts fornecidos que configuram automaticamente o Java 17.

### Usando os Scripts (Recomendado)

```bash
# Compilar o projeto
./compile.sh

# Executar a aplicação
./run.sh
```

### Manualmente com Java 17

```bash
# Configurar Java 17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Verificar versão
java -version  # Deve mostrar Java 17

# Compilar e executar
mvn clean install
mvn spring-boot:run
```

### Using Java JAR

```bash
# Build the JAR file
./compile.sh
mvn package

# Run the JAR
java -jar target/delta-sharing-onprem-1.0.0.jar
```

The application will start on `http://localhost:8080`

## 🌐 Accessing the Application

### Delta Sharing Protocol Endpoints
- **Base URL**: http://localhost:8080/delta-sharing
- **Authentication**: Bearer Token required (see [DELTA_SHARING_README.md](DELTA_SHARING_README.md))
- **Example**: 
  ```bash
  curl -H "Authorization: Bearer your-token" \
    http://localhost:8080/delta-sharing/shares
  ```

### Web Interface
- **Home Page**: http://localhost:8080/
- **Shares Management**: http://localhost:8080/shares

### API Documentation
- **Swagger UI**: http://localhost:8080/swagger-ui.html
- **OpenAPI JSON**: http://localhost:8080/api-docs

### Database Console
- **H2 Console**: http://localhost:8080/h2-console
  - JDBC URL: `jdbc:h2:mem:deltasharing`
  - Username: `sa`
  - Password: (leave empty)

## 📚 API Endpoints

### Delta Sharing Protocol API (`/delta-sharing`)

**Complete Delta Sharing Protocol implementation** - see [DELTA_SHARING_README.md](DELTA_SHARING_README.md) for full documentation.

## 📚 Additional Documentation

- **[DELTA_SHARING_README.md](DELTA_SHARING_README.md)**: Complete Delta Sharing protocol documentation
- **[TESTING.md](TESTING.md)**: Testing guide with practical examples
- **[TREEVIEW_GUIDE.md](TREEVIEW_GUIDE.md)**: Dynamic TreeView component guide (primary navigation)
- **[WEB_INTERFACE_GUIDE.md](WEB_INTERFACE_GUIDE.md)**: Web interface usage guide
- **[test-python/README.md](test-python/README.md)**: Python client test suite documentation

## 🐍 Python Client Testing

A complete Python test suite is available in the `test-python/` directory:

```bash
cd test-python
./setup.sh      # Setup virtual environment and install dependencies
./run_tests.sh  # Run comprehensive test suite
```

The test suite uses the official `delta-sharing` Python library and includes:
- ✅ List shares, schemas, and tables
- ✅ Load table data as Pandas DataFrames
- ✅ Get table metadata and schema
- ✅ Direct REST API testing
- ✅ Example usage patterns

See [test-python/README.md](test-python/README.md) for detailed documentation.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/delta-sharing/shares` | List all shares (Protocol) |
| GET | `/delta-sharing/shares/{share}` | Get share details (Protocol) |
| GET | `/delta-sharing/shares/{share}/schemas` | List schemas (Protocol) |
| GET | `/delta-sharing/shares/{share}/schemas/{schema}/tables` | List tables (Protocol) |
| GET | `/delta-sharing/shares/{share}/all-tables` | List all tables (Protocol) |
| GET | `/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/metadata` | Get table metadata (Protocol) |
| GET | `/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/version` | Get table version (Protocol) |
| POST | `/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/query` | Query table data (Protocol) |
| GET | `/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/changes` | Get table changes/CDF (Protocol) |

**Authentication**: All Delta Sharing endpoints require `Authorization: Bearer <token>` header.

### Management API (`/api/v1/shares`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/shares` | Get all shares |
| GET | `/api/v1/shares/active` | Get active shares |
| GET | `/api/v1/shares/{id}` | Get share by ID |
| GET | `/api/v1/shares/name/{name}` | Get share by name |
| POST | `/api/v1/shares` | Create new share |
| PUT | `/api/v1/shares/{id}` | Update share |
| DELETE | `/api/v1/shares/{id}` | Delete share |

### Example API Calls

#### Get all shares
```bash
curl http://localhost:8080/api/v1/shares
```

#### Create a new share
```bash
curl -X POST http://localhost:8080/api/v1/shares \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-share",
    "description": "My first share",
    "active": true
  }'
```

#### Get share by ID
```bash
curl http://localhost:8080/api/v1/shares/1
```

#### Update a share
```bash
curl -X PUT http://localhost:8080/api/v1/shares/1 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "updated-share",
    "description": "Updated description",
    "active": true
  }'
```

#### Delete a share
```bash
curl -X DELETE http://localhost:8080/api/v1/shares/1
```

## 📁 Project Structure

```
src/
├── main/
│   ├── java/com/databricks/deltasharing/
│   │   ├── DeltaSharingApplication.java    # Main application class
│   │   ├── config/                         # Configuration classes
│   │   │   └── OpenApiConfig.java
│   │   ├── controller/                     # Controllers
│   │   │   ├── api/                        # REST API controllers
│   │   │   │   └── ShareRestController.java
│   │   │   └── web/                        # Web controllers
│   │   │       ├── HomeController.java
│   │   │       └── ShareWebController.java
│   │   ├── dto/                            # Data Transfer Objects
│   │   │   └── ShareDTO.java
│   │   ├── exception/                      # Exception handling
│   │   │   ├── DuplicateResourceException.java
│   │   │   ├── GlobalExceptionHandler.java
│   │   │   └── ResourceNotFoundException.java
│   │   ├── model/                          # Entity models
│   │   │   └── Share.java
│   │   ├── repository/                     # Data repositories
│   │   │   └── ShareRepository.java
│   │   └── service/                        # Business logic
│   │       └── ShareService.java
│   └── resources/
│       ├── application.yml                 # Application configuration
│       ├── static/                         # Static resources
│       │   └── css/
│       │       └── style.css
│       └── templates/                      # Thymeleaf templates
│           ├── index.html
│           └── shares/
│               ├── form.html
│               └── list.html
└── test/                                   # Test files
```

## ⚙️ Configuration

The application can be configured via `src/main/resources/application.yml`:

```yaml
server:
  port: 8080                    # Server port

spring:
  datasource:
    url: jdbc:h2:mem:deltasharing  # Database URL
    username: sa
    password: 
  
  jpa:
    hibernate:
      ddl-auto: update          # Auto-create/update schema
    show-sql: true              # Show SQL queries in logs
```

### Using PostgreSQL in Production

Update `application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/deltasharing
    username: your_username
    password: your_password
  jpa:
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
```

## 🧪 Testing

```bash
# Run tests
mvn test

# Run tests with coverage
mvn clean test jacoco:report
```

## 📦 Building for Production

```bash
# Build production JAR
mvn clean package -DskipTests

# The JAR will be created in target/delta-sharing-onprem-1.0.0.jar
```

## 🐳 Docker Support (Optional)

Create a `Dockerfile`:

```dockerfile
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY target/delta-sharing-onprem-1.0.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Build and run:

```bash
docker build -t delta-sharing-onprem .
docker run -p 8080:8080 delta-sharing-onprem
```

## 🔒 Security Considerations

This is a development template. For production:

1. Add Spring Security for authentication/authorization
2. Configure HTTPS/SSL
3. Use environment variables for sensitive data
4. Implement rate limiting
5. Add CORS configuration as needed
6. Use a production-grade database
7. Configure proper logging and monitoring

## 📝 License

Apache 2.0

## 👥 Contributing

Contributions are welcome\! Please feel free to submit a Pull Request.

## 📧 Support

For issues and questions, please open an issue on the repository.

---

Built with ❤️ using Spring Boot
