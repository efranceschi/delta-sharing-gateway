# Delta Sharing Protocol Test Suite

Script Python para testar todos os endpoints da API Delta Sharing conforme a especificação oficial do protocolo.

## 📋 Funcionalidades

✅ Testa **todos os endpoints** do Delta Sharing Protocol:
- List Shares
- Get Share
- List Schemas
- List Tables
- List All Tables
- Query Table Version
- Query Table Metadata
- Query Table Data
- Query Table Changes (CDF)

✅ Múltiplas variações de teste:
- Paginação (maxResults, pageToken)
- Headers especiais (Delta-Sharing-Capabilities, includeEndStreamAction)
- Formatos diferentes (parquet, delta)
- Predicados e limites

✅ Display detalhado:
- Método HTTP
- URL completa
- Request headers
- Request body (JSON formatado com 2 espaços)
- Response headers
- Response body (JSON/NDJSON formatado com 2 espaços)
- Tempo de execução
- Status do teste (PASS/FAIL)

## 🚀 Como Usar

### Pré-requisitos

```bash
pip install requests
```

### ⚡ Inicialização Automática (Obrigatória)

Antes de executar qualquer teste, o script realiza uma **inicialização automática obrigatória**:

1. **Lista todos os shares** disponíveis (`GET /shares`)
2. **Descobre schemas e tabelas** usando o endpoint `/shares/{share}/all-tables`
3. **Valida** se há recursos suficientes (pelo menos 1 share, 1 schema, 1 table)
4. **Exibe resumo** dos recursos descobertos
5. **Executa os testes** usando os recursos identificados

Isso garante que todos os testes tenham os recursos necessários antes de começar, evitando falhas desnecessárias.

### Execução Básica

```bash
# Executar TODOS os testes
python test_delta_sharing_protocol.py config.share

# Listar testes disponíveis
python test_delta_sharing_protocol.py config.share --list-tests

# Executar um teste ESPECÍFICO
python test_delta_sharing_protocol.py config.share -t 1
python test_delta_sharing_protocol.py config.share -t 8.5

# Usando credenciais de admin
python test_delta_sharing_protocol.py admin-credentials.share

# Ou executando diretamente (se chmod +x foi aplicado)
./test_delta_sharing_protocol.py config.share
```

### Parâmetros da Linha de Comando

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| `config_file` | Arquivo de configuração .share (obrigatório) | `config.share` |
| `-t, --test TEST_ID` | Executar apenas um teste específico | `-t 8.5` |
| `--list-tests` | Listar todos os testes disponíveis | `--list-tests` |
| `-h, --help` | Mostrar ajuda | `-h` |

### Formato do Arquivo .share

O arquivo de configuração deve seguir o formato Delta Sharing:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "http://localhost:8080/delta-sharing",
  "bearerToken": "dss_0000000000000000000000000000000000000000000000000000000000000"
}
```

## 📊 Exemplo de Saída

```
================================================================================
  DELTA SHARING PROTOCOL TEST SUITE
================================================================================

Testing endpoint: http://localhost:8080/delta-sharing
Started at: 2025-10-15 17:30:00

Initializing test resources...

Step 1: Discovering shares...
✓ Found 1 share(s)
  Using share: share01

Step 2: Discovering schemas and tables via all-tables endpoint...
✓ Found 1 schema(s) and 3 table(s)
  Using schema: schema01
  Using table: parquet01

Discovered Resources Summary:
  Share: share01
  Schemas: schema01
  Tables (3):
    - schema01.parquet01
    - schema01.delta01
    - schema01.iceberg01

✓ Initialization completed successfully

================================================================================

Test 1: List Shares
GET /shares
--------------------------------------------------------------------------------

REQUEST:
URL: http://localhost:8080/delta-sharing/shares
Headers:
  Authorization: Bearer dss_***
  Content-Type: application/json

EXECUTING...

RESPONSE:
Status Code: 200 OK
Duration: 0.125 seconds
Response Headers:
  Content-Type: application/json
  includeEndStreamAction: true

Response Body:
{
  "items": [
    {
      "name": "share01",
      "id": "495d999f-fb06-4dd6-9a13-c2c1691a83a6"
    }
  ]
}

✓ TEST PASSED

📌 Discovered share: share01

...

================================================================================

TEST SUMMARY

Overall Statistics:
  Total tests executed: 21
  ✓ Passed: 19
  ⚠ Warning: 1
  ✗ Failed: 1
  Total duration: 5.234 seconds

Detailed Results:

  Test   Status     Status Code  Duration     Description
  ------ ---------- ------------ ------------ ----------------------------------------
  1      ✓ PASSED   200          0.125s       List Shares
  1.1    ✓ PASSED   200          0.098s       List Shares (Paginated)
  2      ✓ PASSED   200          0.087s       Get Share
  3      ✓ PASSED   200          0.092s       List Schemas
  3.1    ✓ PASSED   200          0.089s       List Schemas (Paginated)
  4      ✓ PASSED   200          0.095s       List Tables
  4.1    ✓ PASSED   200          0.091s       List Tables (Paginated)
  5      ✓ PASSED   200          0.102s       List All Tables
  5.1    ✓ PASSED   200          0.096s       List All Tables (Paginated)
  6      ✓ PASSED   200          0.088s       Query Table Version
  7      ✓ PASSED   200          0.156s       Query Table Metadata (Basic)
  7.1    ✓ PASSED   200          0.162s       Query Table Metadata (with Capabilities)
  7.2    ✓ PASSED   200          0.159s       Query Table Metadata (with EndStreamAct
  8      ✓ PASSED   200          0.526s       Query Table Data (Basic)
  8.1    ✓ PASSED   200          0.198s       Query Table Data (with limitHint)
  8.2    ✓ PASSED   200          0.523s       Query Table Data (with version)
  8.3    ✓ PASSED   200          0.519s       Query Table Data (Parquet format)
  8.4    ✓ PASSED   200          0.515s       Query Table Data (Delta format)
  8.5    ✓ PASSED   200          0.528s       Query Table Data (with EndStreamAction)
  8.6    ✓ PASSED   200          0.521s       Query Table Data (with predicateHints)
  9      ⚠ WARNING  404          0.089s       Query Table Changes (CDF)

Failed Tests Details:
  (none)

Completed at: 2025-10-15 17:35:15

================================================================================
```

## 🔍 Testes Incluídos (29 testes)

### Testes Básicos (11 testes)
- **Test 1**: List Shares
- **Test 1.1**: List Shares (Paginated)
- **Test 2**: Get Share
- **Test 3**: List Schemas
- **Test 3.1**: List Schemas (Paginated)
- **Test 4**: List Tables in Schema
- **Test 4.1**: List Tables (Paginated)
- **Test 5**: List All Tables
- **Test 5.1**: List All Tables (Paginated)
- **Test 6**: Query Table Version

### Testes de Metadata (5 testes)
- **Test 7**: Query Table Metadata (Basic)
- **Test 7.1**: Query Table Metadata (Parquet format)
- **Test 7.2**: Query Table Metadata (Delta format)
- **Test 7.3**: Query Table Metadata (Parquet + EndStreamAction)
- **Test 7.4**: Query Table Metadata (Delta + EndStreamAction)

### Testes de Query Table Data (11 testes)
- **Test 8**: Query Table Data (Basic)
- **Test 8.1**: Query Table Data (Parquet format)
- **Test 8.2**: Query Table Data (Delta format)
- **Test 8.3**: Query Table Data (Parquet + limitHint)
- **Test 8.4**: Query Table Data (Delta + limitHint)
- **Test 8.5**: Query Table Data (Parquet + version)
- **Test 8.6**: Query Table Data (Delta + version)
- **Test 8.7**: Query Table Data (Parquet + EndStreamAction)
- **Test 8.8**: Query Table Data (Delta + EndStreamAction)
- **Test 8.9**: Query Table Data (Parquet + predicateHints)
- **Test 8.10**: Query Table Data (Delta + predicateHints)

### Testes de CDF (2 testes)
- **Test 9**: Query Table Changes (CDF - Parquet format)
- **Test 9.1**: Query Table Changes (CDF - Delta format)

### 🎯 Cobertura por Formato

Cada teste que pode ter comportamento diferente entre formatos foi desdobrado:

| Funcionalidade | Parquet | Delta | Total |
|----------------|---------|-------|-------|
| Query Metadata | 7.1, 7.3 | 7.2, 7.4 | 4 |
| Query Data | 8.1, 8.3, 8.5, 8.7, 8.9 | 8.2, 8.4, 8.6, 8.8, 8.10 | 10 |
| Query Changes (CDF) | 9 | 9.1 | 2 |

**Por que separar?** O comportamento da API pode ser diferente:
- **Parquet**: `{"protocol":{"minReaderVersion":1}}`
- **Delta**: `{"protocol":{"deltaProtocol":{"minReaderVersion":1}}}`

## 🎯 Comparando Servidores

Para comparar dois servidores Delta Sharing:

```bash
# Servidor 1 (local)
python test_delta_sharing_protocol.py config.share > results_local.txt

# Servidor 2 (Databricks)
python test_delta_sharing_protocol.py databricks.share > results_databricks.txt

# Comparar resultados
diff results_local.txt results_databricks.txt
```

## 📚 Referência

Este script segue a especificação oficial do Delta Sharing Protocol:
- https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md

## 🐛 Troubleshooting

### Erro de Conexão
```
✗ TEST FAILED: Connection refused
```
**Solução**: Verifique se o servidor está rodando e o endpoint está correto no arquivo `.share`.

### Erro 401 Unauthorized
```
Response Status: 401 Unauthorized
```
**Solução**: Verifique se o `bearerToken` no arquivo `.share` está correto.

### Falha na inicialização
```
✗ Initialization failed. Cannot proceed with tests.
⚠ Please ensure the server is running and has at least one share with tables.
```
**Solução**: 
- Verifique se o servidor está rodando
- Confirme que há pelo menos 1 share configurado
- Confirme que o share tem pelo menos 1 tabela
- Verifique as credenciais no arquivo `.share`
- Teste a conectividade com o servidor

### Nenhum recurso descoberto
```
⚠ Skipping test: No share discovered
```
**Solução**: Este erro não deve mais ocorrer se a inicialização for bem-sucedida. Se ocorrer, há um bug no script.

