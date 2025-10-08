# File Storage Services

Este pacote contém as implementações de armazenamento de arquivos para o Delta Sharing Server.

## 📁 Arquivos

### FileStorageService.java
Interface principal que define o contrato para todos os serviços de armazenamento.

**Métodos:**
- `getTableFiles()` - Retorna lista de FileResponse para uma tabela
- `getStorageType()` - Retorna identificador do tipo de storage
- `isAvailable()` - Verifica se o serviço está disponível

### FakeFileStorageService.java
Implementação fake para testes e desenvolvimento.

**Características:**
- Gera dados fictícios automaticamente
- Não requer infraestrutura externa
- Sempre disponível
- Ativação: `delta.sharing.storage.type=fake` (padrão)

### MinIOFileStorageService.java
Implementação para MinIO/S3 com URLs pré-assinadas.

**Características:**
- Integração com MinIO/S3
- URLs pré-assinadas com expiração configurável
- Extração automática de partições
- Metadados reais dos arquivos
- Ativação: `delta.sharing.storage.type=minio`

**Configuração:**
```yaml
delta:
  sharing:
    storage:
      type: minio
      minio:
        endpoint: http://localhost:9000
        access-key: minioadmin
        secret-key: minioadmin
        bucket: delta-sharing
        url-expiration-minutes: 60
```

### HttpFileStorageService.java
Implementação para servidor HTTP genérico.

**Características:**
- URLs permanentes (sem expiração)
- Serve arquivos de filesystem local
- Extração automática de partições
- Metadados reais dos arquivos
- Ativação: `delta.sharing.storage.type=http`

**Configuração:**
```yaml
delta:
  sharing:
    storage:
      type: http
      http:
        base-url: http://localhost:8080/files
        base-path: /tmp/delta-tables
```

## 🏗️ Arquitetura

```
FileStorageService (interface)
    ├── FakeFileStorageService
    ├── MinIOFileStorageService
    └── HttpFileStorageService
```

## 🔧 Como Usar

### Seleção Automática
O Spring Boot seleciona automaticamente a implementação correta baseado na propriedade `delta.sharing.storage.type`:

```java
@ConditionalOnProperty(name = "delta.sharing.storage.type", havingValue = "fake")
public class FakeFileStorageService implements FileStorageService { ... }
```

### Injeção de Dependência
```java
@Service
public class DeltaSharingService {
    private final FileStorageService fileStorageService;
    
    // Spring injeta automaticamente a implementação correta
    public DeltaSharingService(FileStorageService fileStorageService) {
        this.fileStorageService = fileStorageService;
    }
}
```

## 🚀 Adicionar Novo Storage

Para adicionar um novo tipo de storage (ex: AWS S3, Azure Blob, GCS):

1. **Criar classe implementando FileStorageService**
```java
@Service
@ConditionalOnProperty(name = "delta.sharing.storage.type", havingValue = "s3")
@ConfigurationProperties(prefix = "delta.sharing.storage.s3")
public class S3FileStorageService implements FileStorageService {
    
    @Override
    public List<FileResponse> getTableFiles(DeltaTable table, Long version, 
                                             List<String> predicateHints, Integer limitHint) {
        // Implementação
    }
    
    @Override
    public String getStorageType() {
        return "s3";
    }
    
    @Override
    public boolean isAvailable() {
        // Validação
    }
}
```

2. **Adicionar configurações no application.yml**
```yaml
delta:
  sharing:
    storage:
      s3:
        region: us-east-1
        bucket: my-bucket
        # ...
```

3. **Adicionar dependências no pom.xml**
```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>s3</artifactId>
    <version>2.x.x</version>
</dependency>
```

4. **Documentar**
- Adicionar exemplo em `config-examples/`
- Atualizar `FILE_STORAGE_CONFIGURATION.md`
- Atualizar `STORAGE_QUICKSTART.md`

## 📚 Documentação

Para mais detalhes, consulte:
- [STORAGE_QUICKSTART.md](../../../../../../../STORAGE_QUICKSTART.md) - Quick start guide
- [FILE_STORAGE_CONFIGURATION.md](../../../../../../../FILE_STORAGE_CONFIGURATION.md) - Documentação completa
- [TESTING_EXAMPLES.md](../../../../../../../TESTING_EXAMPLES.md) - Exemplos de teste
- [IMPLEMENTATION_SUMMARY.md](../../../../../../../IMPLEMENTATION_SUMMARY.md) - Detalhes técnicos

## 🧪 Testes

```bash
# Testar storage específico
./test-storage.sh fake
./test-storage.sh minio
./test-storage.sh http

# Ou usar curl diretamente
curl -X POST \
  -H "Authorization: Bearer test-token" \
  -H "Content-Type: application/json" \
  -d '{"limitHint": 5}' \
  http://localhost:8080/delta-sharing/shares/share1/schemas/schema1/tables/table1/query
```

## 🎯 Padrões de Design

### Strategy Pattern
A interface `FileStorageService` define o contrato, e múltiplas implementações podem ser trocadas em runtime.

### Dependency Injection
Spring gerencia a criação e injeção da implementação correta.

### Configuration Properties
Type-safe configuration com `@ConfigurationProperties`.

### Conditional Beans
`@ConditionalOnProperty` permite ativar/desativar implementações via configuração.

## 📊 Comparação

| Storage | Produção | URLs Seguras | Setup | Arquivos Reais |
|---------|----------|--------------|-------|----------------|
| Fake    | ❌       | ✅ (fake)    | ⚡ Instantâneo | ❌ |
| MinIO   | ✅       | ✅ (pre-signed) | 🔧 5 min | ✅ |
| HTTP    | ⚠️       | ❌           | 🔧 2 min | ✅ |

## 🔍 Troubleshooting

### "Storage service is not available"
```bash
# Verificar configuração
env | grep DELTA_SHARING_STORAGE_TYPE

# Verificar logs
tail -f logs/delta-sharing.log | grep -i storage
```

### "No files returned"
```bash
# Verificar location da tabela
curl http://localhost:8080/api/v1/tables | jq '.[] | {name, location}'

# Verificar se arquivos existem (MinIO/HTTP)
# MinIO:
docker run --rm --network host minio/mc ls myminio/delta-sharing/table1/

# HTTP:
ls -la /tmp/delta-tables/table1/
```

## ✨ Features

- ✅ Pluggable architecture
- ✅ Easy to extend
- ✅ Type-safe configuration
- ✅ Automatic partition extraction
- ✅ Real file metadata
- ✅ Pre-signed URLs (MinIO)
- ✅ Health checks
- ✅ Comprehensive logging
- ✅ Error handling
