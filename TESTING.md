# Guia de Testes - Delta Sharing Protocol

Este guia fornece exemplos práticos para testar a implementação do Delta Sharing Protocol.

## 🚀 Iniciando o Servidor

```bash
mvn spring-boot:run
```

O servidor estará disponível em `http://localhost:8080`

## 🔑 Configuração de Autenticação

### Opção 1: Modo Desenvolvimento (Padrão)
Por padrão, o servidor aceita qualquer token não vazio:

```bash
TOKEN="test-token"
```

### Opção 2: Token Específico
Configure um token específico no `application.yml` ou via variável de ambiente:

```bash
export DELTA_SHARING_TOKEN="my-secret-token-123"
TOKEN="my-secret-token-123"
```

### Opção 3: Desabilitar Autenticação (Desenvolvimento)
No `application.yml`:
```yaml
delta:
  sharing:
    auth:
      enabled: false
```

## 📊 Dados de Exemplo

O servidor é inicializado com dados de exemplo:

- **Shares**: `demo-share`, `sales-share`
- **Schemas**: `default`, `analytics`, `sales`
- **Tables**: `customers`, `orders`, `revenue`, `transactions`

## 🧪 Testes com cURL

### 1. Listar Shares

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares | jq
```

**Resposta esperada:**
```json
{
  "items": [
    {
      "name": "demo-share",
      "id": "1"
    },
    {
      "name": "sales-share",
      "id": "2"
    }
  ],
  "nextPageToken": null
}
```

### 2. Obter Share Específico

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share | jq
```

**Resposta esperada:**
```json
{
  "name": "demo-share",
  "id": "1"
}
```

### 3. Listar Schemas

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas | jq
```

**Resposta esperada:**
```json
{
  "items": [
    {
      "name": "default",
      "share": "demo-share"
    },
    {
      "name": "analytics",
      "share": "demo-share"
    }
  ],
  "nextPageToken": null
}
```

### 4. Listar Tabelas

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas/default/tables | jq
```

**Resposta esperada:**
```json
{
  "items": [
    {
      "name": "customers",
      "schema": "default",
      "share": "demo-share",
      "shareAsView": false,
      "id": "1"
    },
    {
      "name": "orders",
      "schema": "default",
      "share": "demo-share",
      "shareAsView": false,
      "id": "2"
    }
  ],
  "nextPageToken": null
}
```

### 5. Listar Todas as Tabelas

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/all-tables | jq
```

### 6. Obter Metadados da Tabela

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas/default/tables/customers/metadata
```

**Resposta esperada (NDJSON):**
```
{"protocol":{"minReaderVersion":1}}
{"metaData":{"id":"1","name":"customers","format":{"provider":"parquet"},...}}
```

### 7. Obter Versão da Tabela

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas/default/tables/customers/version | jq
```

**Resposta esperada:**
```json
{
  "deltaTableVersion": 0
}
```

### 8. Consultar Dados da Tabela

```bash
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"limitHint": 100}' \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas/default/tables/customers/query
```

**Com predicados:**
```bash
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "predicateHints": ["date >= '\''2023-01-01'\''"],
    "limitHint": 1000,
    "version": 0
  }' \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas/default/tables/customers/query
```

### 9. Verificar Headers

```bash
curl -v -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares 2>&1 | grep -i "delta-sharing"
```

Deve retornar:
```
< Delta-Sharing-Capabilities: responseformat=parquet,delta
```

## 🐍 Testes com Python

### Instalação do Cliente

```bash
pip install delta-sharing
```

### Criar Arquivo de Configuração

Crie um arquivo `config.share`:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "http://localhost:8080/delta-sharing",
  "bearerToken": "test-token"
}
```

### Script de Teste

```python
import delta_sharing
import json

# Criar profile
profile_file = "config.share"

# Criar cliente
client = delta_sharing.SharingClient(profile_file)

# 1. Listar shares
print("=== Shares ===")
shares = client.list_shares()
for share in shares:
    print(f"  - {share.name}")

# 2. Listar schemas
print("\n=== Schemas in demo-share ===")
schemas = client.list_schemas(delta_sharing.Share(name="demo-share"))
for schema in schemas:
    print(f"  - {schema.name}")

# 3. Listar tabelas
print("\n=== Tables in demo-share.default ===")
tables = client.list_tables(
    delta_sharing.Schema(name="default", share="demo-share")
)
for table in tables:
    print(f"  - {table.name}")

# 4. Carregar tabela em Pandas
print("\n=== Loading table as Pandas DataFrame ===")
try:
    table_url = f"{profile_file}#demo-share.default.customers"
    df = delta_sharing.load_as_pandas(table_url)
    print(f"DataFrame shape: {df.shape}")
    print(df.head())
except Exception as e:
    print(f"Note: Data loading requires actual Parquet files: {e}")
```

Executar:
```bash
python test_delta_sharing.py
```

## ⚡ Testes com Apache Spark

### Configuração

Adicione a dependência no seu projeto Spark:

```scala
libraryDependencies += "io.delta" %% "delta-sharing-spark" % "1.0.0"
```

### Script de Teste

```scala
import io.delta.sharing.spark._
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Delta Sharing Test")
  .master("local[*]")
  .getOrCreate()

// Configurar profile
val profileFile = "config.share"

// 1. Listar shares
println("=== Shares ===")
spark.sql(s"SHOW SHARES IN deltaSharing.`$profileFile`").show()

// 2. Listar schemas
println("=== Schemas ===")
spark.sql(s"SHOW SCHEMAS IN deltaSharing.`$profileFile`.`demo-share`").show()

// 3. Listar tabelas
println("=== Tables ===")
spark.sql(s"SHOW TABLES IN deltaSharing.`$profileFile`.`demo-share`.`default`").show()

// 4. Ler tabela
val tablePath = s"$profileFile#demo-share.default.customers"
val df = spark.read.format("deltaSharing").load(tablePath)
df.show()
```

## 🔍 Testes de Erro

### 1. Sem Autenticação

```bash
curl http://localhost:8080/delta-sharing/shares
```

**Resposta esperada:**
```json
{
  "errorCode": "UNAUTHENTICATED",
  "message": "Missing or invalid Authorization header"
}
```

### 2. Token Inválido

```bash
curl -H "Authorization: Bearer invalid-token" \
  http://localhost:8080/delta-sharing/shares
```

**Resposta esperada (se token específico configurado):**
```json
{
  "errorCode": "UNAUTHENTICATED",
  "message": "Invalid bearer token"
}
```

### 3. Share Não Encontrado

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/nonexistent-share
```

**Resposta esperada:**
```json
{
  "status": 404,
  "message": "Share not found: nonexistent-share",
  "timestamp": "2025-10-05T..."
}
```

### 4. Schema Não Encontrado

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/demo-share/schemas/nonexistent/tables
```

**Resposta esperada:**
```json
{
  "status": 404,
  "message": "Schema not found: nonexistent in share: demo-share",
  "timestamp": "2025-10-05T..."
}
```

## 📝 Testes de Integração

### Script Completo de Teste

```bash
#!/bin/bash

# Configuração
BASE_URL="http://localhost:8080/delta-sharing"
TOKEN="test-token"
HEADERS="-H 'Authorization: Bearer $TOKEN'"

echo "🧪 Testando Delta Sharing Protocol Implementation"
echo "================================================"

# Teste 1: Listar Shares
echo -e "\n✅ Teste 1: Listar Shares"
curl -s $HEADERS "$BASE_URL/shares" | jq -r '.items[].name'

# Teste 2: Listar Schemas
echo -e "\n✅ Teste 2: Listar Schemas"
curl -s $HEADERS "$BASE_URL/shares/demo-share/schemas" | jq -r '.items[].name'

# Teste 3: Listar Tabelas
echo -e "\n✅ Teste 3: Listar Tabelas"
curl -s $HEADERS "$BASE_URL/shares/demo-share/schemas/default/tables" | jq -r '.items[].name'

# Teste 4: Metadados
echo -e "\n✅ Teste 4: Obter Metadados"
curl -s $HEADERS "$BASE_URL/shares/demo-share/schemas/default/tables/customers/metadata" | head -n 2

# Teste 5: Versão
echo -e "\n✅ Teste 5: Obter Versão"
curl -s $HEADERS "$BASE_URL/shares/demo-share/schemas/default/tables/customers/version" | jq

# Teste 6: Query
echo -e "\n✅ Teste 6: Query de Dados"
curl -s -X POST $HEADERS \
  -H "Content-Type: application/json" \
  -d '{"limitHint": 10}' \
  "$BASE_URL/shares/demo-share/schemas/default/tables/customers/query" | head -n 2

echo -e "\n\n✅ Todos os testes concluídos\!"
```

Salve como `test_delta_sharing.sh` e execute:
```bash
chmod +x test_delta_sharing.sh
./test_delta_sharing.sh
```

## 📊 Verificação de Compatibilidade

### Checklist de Compatibilidade

- ✅ Endpoints seguem especificação Delta Sharing Protocol
- ✅ Autenticação Bearer Token implementada
- ✅ Headers `Delta-Sharing-Capabilities` incluídos
- ✅ Formato NDJSON para metadata e query
- ✅ Estrutura de resposta compatível com clientes oficiais
- ✅ Códigos de erro HTTP corretos
- ✅ Mensagens de erro no formato esperado

### Teste de Compatibilidade com Cliente Python

```python
# Este teste verifica se a implementação é compatível com o cliente oficial
import delta_sharing

profile = {
    "shareCredentialsVersion": 1,
    "endpoint": "http://localhost:8080/delta-sharing",
    "bearerToken": "test-token"
}

# Salvar profile
with open("config.share", "w") as f:
    import json
    json.dump(profile, f)

# Testar compatibilidade
client = delta_sharing.SharingClient("config.share")

# Se estes comandos funcionarem, a implementação é compatível
shares = client.list_shares()
assert len(shares) > 0, "Deve retornar shares"

schemas = client.list_schemas(shares[0])
assert len(schemas) > 0, "Deve retornar schemas"

print("✅ Implementação compatível com cliente Delta Sharing oficial\!")
```

## 🎯 Próximos Passos

1. **Adicionar Dados Reais**: Integrar com Delta Lake ou Parquet files
2. **Implementar Pre-signed URLs**: Para acesso seguro aos arquivos
3. **Testar com Spark**: Validar integração completa com Apache Spark
4. **Performance**: Testar com grandes volumes de dados
5. **Segurança**: Implementar tokens JWT e rate limiting

## 📚 Recursos

- [Delta Sharing Protocol](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md)
- [Python Client](https://github.com/delta-io/delta-sharing/tree/main/python)
- [Spark Connector](https://github.com/delta-io/delta-sharing/tree/main/spark)
