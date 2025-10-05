# Delta Sharing Protocol Implementation

Este projeto implementa um servidor **Delta Sharing** 100% em Java, compatível com a especificação oficial do protocolo Delta Sharing.

## 📖 Sobre o Delta Sharing

Delta Sharing é um protocolo aberto para compartilhamento seguro de dados em larga escala, desenvolvido pela Databricks. Ele permite que organizações compartilhem dados em tempo real sem a necessidade de copiar ou mover os dados.

**Referências:**
- [Delta Sharing Protocol Specification](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md)
- [Delta Sharing GitHub Repository](https://github.com/delta-io/delta-sharing)
- [Delta.io Sharing](https://delta.io/sharing)

## 🏗️ Arquitetura

Esta implementação segue fielmente a especificação do protocolo Delta Sharing e é 100% compatível com clientes Delta Sharing existentes.

### Componentes Principais

1. **Modelos de Domínio**
   - `DeltaShare`: Representa um compartilhamento lógico
   - `DeltaSchema`: Representa um schema dentro de um share
   - `DeltaTable`: Representa uma tabela dentro de um schema

2. **DTOs do Protocolo**
   - `ShareResponse`, `ListSharesResponse`
   - `SchemaResponse`, `ListSchemasResponse`
   - `TableResponse`, `ListTablesResponse`
   - `ProtocolResponse`, `FormatResponse`, `MetadataResponse`
   - `FileResponse`, `QueryTableRequest`

3. **Serviços**
   - `DeltaSharingService`: Implementa a lógica do protocolo

4. **Segurança**
   - `BearerTokenAuthenticationFilter`: Autenticação via Bearer Token
   - `SecurityConfig`: Configuração de segurança Spring

## 🔌 Endpoints REST API

Todos os endpoints seguem a especificação oficial do Delta Sharing Protocol.

### Autenticação

Todos os endpoints Delta Sharing requerem autenticação via Bearer Token:

```bash
Authorization: Bearer <your-token>
```

### Endpoints Implementados

#### 1. Listar Shares
```
GET /delta-sharing/shares
```

**Resposta:**
```json
{
  "items": [
    {
      "name": "share1",
      "id": "1"
    }
  ],
  "nextPageToken": null
}
```

#### 2. Obter Share Específico
```
GET /delta-sharing/shares/{share}
```

**Resposta:**
```json
{
  "name": "share1",
  "id": "1"
}
```

#### 3. Listar Schemas
```
GET /delta-sharing/shares/{share}/schemas
```

**Resposta:**
```json
{
  "items": [
    {
      "name": "schema1",
      "share": "share1"
    }
  ],
  "nextPageToken": null
}
```

#### 4. Listar Tabelas em um Schema
```
GET /delta-sharing/shares/{share}/schemas/{schema}/tables
```

**Resposta:**
```json
{
  "items": [
    {
      "name": "table1",
      "schema": "schema1",
      "share": "share1",
      "shareAsView": false,
      "id": "1"
    }
  ],
  "nextPageToken": null
}
```

#### 5. Listar Todas as Tabelas
```
GET /delta-sharing/shares/{share}/all-tables
```

#### 6. Obter Metadados da Tabela
```
GET /delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/metadata
```

**Resposta:** NDJSON (Newline-Delimited JSON)
```
{"protocol":{"minReaderVersion":1}}
{"metaData":{"id":"1","name":"table1","format":{"provider":"parquet"},...}}
```

#### 7. Obter Versão da Tabela
```
GET /delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/version
```

**Resposta:**
```json
{
  "deltaTableVersion": 0
}
```

#### 8. Consultar Dados da Tabela
```
POST /delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/query
Content-Type: application/json

{
  "predicateHints": ["date >= '2023-01-01'"],
  "limitHint": 1000,
  "version": 0
}
```

**Resposta:** NDJSON com protocol, metadata e file references

#### 9. Consultar Mudanças (CDF)
```
GET /delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/changes?startingVersion=0
```

## 🔐 Configuração de Autenticação

### Configuração via `application.yml`

```yaml
delta:
  sharing:
    auth:
      enabled: true
      bearer-token: "your-secret-token-here"
```

### Configuração via Variável de Ambiente

```bash
export DELTA_SHARING_TOKEN="your-secret-token-here"
```

### Modo de Desenvolvimento

Se nenhum token for configurado, o servidor aceita qualquer token não vazio (útil para desenvolvimento):

```yaml
delta:
  sharing:
    auth:
      enabled: true
      bearer-token: ""  # Aceita qualquer token
```

Para desabilitar autenticação completamente (NÃO recomendado para produção):

```yaml
delta:
  sharing:
    auth:
      enabled: false
```

## 🚀 Exemplos de Uso

### Usando cURL

```bash
# Definir token
TOKEN="your-secret-token"

# Listar shares
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares

# Listar schemas
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/share1/schemas

# Listar tabelas
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/share1/schemas/schema1/tables

# Obter metadados
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares/share1/schemas/schema1/tables/table1/metadata

# Consultar dados
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"limitHint": 100}' \
  http://localhost:8080/delta-sharing/shares/share1/schemas/schema1/tables/table1/query
```

### Usando Python (delta-sharing client)

```python
import delta_sharing

# Criar arquivo de configuração do profile
profile = {
    "shareCredentialsVersion": 1,
    "endpoint": "http://localhost:8080/delta-sharing",
    "bearerToken": "your-secret-token"
}

# Salvar profile
import json
with open("config.share", "w") as f:
    json.dump(profile, f)

# Usar o cliente Delta Sharing
client = delta_sharing.SharingClient("config.share")

# Listar shares
shares = client.list_shares()
print(shares)

# Listar schemas
schemas = client.list_schemas("share1")
print(schemas)

# Listar tabelas
tables = client.list_tables("share1", "schema1")
print(tables)

# Carregar dados em Pandas
df = delta_sharing.load_as_pandas(
    "config.share#share1.schema1.table1"
)
print(df.head())
```

### Usando Apache Spark

```scala
import io.delta.sharing.spark._

val profileFile = "config.share"
val tablePath = s"$profileFile#share1.schema1.table1"

// Ler como DataFrame
val df = spark.read.format("deltaSharing").load(tablePath)
df.show()

// Time Travel
val dfVersion = spark.read
  .format("deltaSharing")
  .option("versionAsOf", "0")
  .load(tablePath)
```

## 📊 Estrutura do Banco de Dados

### Tabelas

1. **delta_shares**
   - `id`: Primary key
   - `name`: Nome único do share
   - `description`: Descrição
   - `active`: Status ativo/inativo
   - `created_at`, `updated_at`: Timestamps

2. **delta_schemas**
   - `id`: Primary key
   - `name`: Nome do schema
   - `description`: Descrição
   - `share_id`: Foreign key para delta_shares
   - `created_at`, `updated_at`: Timestamps

3. **delta_tables**
   - `id`: Primary key
   - `name`: Nome da tabela
   - `description`: Descrição
   - `schema_id`: Foreign key para delta_schemas
   - `share_as_view`: Boolean
   - `location`: Localização dos dados
   - `table_format`: Formato (parquet, delta)
   - `created_at`, `updated_at`: Timestamps

## 🧪 Testando a Implementação

### 1. Popular o Banco de Dados

Use a interface web ou API REST para criar shares, schemas e tables:

```bash
# Criar um share
curl -X POST http://localhost:8080/api/v1/shares \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-share",
    "description": "My first share",
    "active": true
  }'
```

### 2. Testar Endpoints Delta Sharing

```bash
TOKEN="test-token"

# Verificar se o share aparece
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/delta-sharing/shares
```

## 🔄 Compatibilidade

Esta implementação é compatível com:

- ✅ Delta Sharing Protocol v1
- ✅ Cliente Python delta-sharing
- ✅ Apache Spark com delta-sharing connector
- ✅ Pandas via delta-sharing
- ✅ Formato NDJSON para respostas de metadata e files
- ✅ Autenticação Bearer Token
- ✅ Paginação (estrutura pronta, implementação básica)
- ⚠️ Time Travel (estrutura pronta, implementação parcial)
- ⚠️ Change Data Feed (estrutura pronta, não implementado)

## 🛠️ Próximos Passos para Produção

1. **Integração com Delta Lake**
   - Implementar leitura real de transaction logs do Delta Lake
   - Gerar URLs pré-assinadas para arquivos Parquet
   - Suporte completo para Time Travel

2. **Change Data Feed (CDF)**
   - Implementar suporte completo para CDC

3. **Otimizações**
   - Implementar paginação real
   - Cache de metadados
   - Pool de conexões otimizado

4. **Segurança**
   - Suporte para JWT tokens
   - Integração com sistemas de autenticação externos
   - Rate limiting
   - Audit logging

5. **Armazenamento**
   - Suporte para S3, Azure Blob Storage, GCS
   - Geração de pre-signed URLs
   - Suporte para diferentes formatos de credenciais

## 📚 Recursos Adicionais

- [Delta Sharing Protocol Documentation](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md)
- [Delta Sharing Python Client](https://github.com/delta-io/delta-sharing/tree/main/python)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Parquet](https://parquet.apache.org/)

## 🤝 Contribuindo

Esta implementação segue a especificação oficial do Delta Sharing Protocol. Para contribuir:

1. Verifique a especificação oficial
2. Mantenha compatibilidade com clientes existentes
3. Adicione testes para novas funcionalidades
4. Documente mudanças no código

## 📝 Licença

Apache 2.0 - Compatível com o projeto Delta Sharing original.
