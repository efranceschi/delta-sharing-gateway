# Delta Sharing OnPrem - Python Test Suite

This directory contains a simple Python application to test the Delta Sharing OnPrem server using the official `delta-sharing` library.

## 📋 Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- Delta Sharing OnPrem server running at `http://localhost:8080`

## 🚀 Quick Setup

### 1. Configure the environment

```bash
cd test-python
chmod +x setup.sh run_tests.sh
./setup.sh
```

This script will:
- Create a Python virtual environment
- Install all necessary dependencies
- Configure the test environment

### 2. Run the tests

```bash
./run_tests.sh
```

Or manually:

```bash
source venv/bin/activate
python test_delta_sharing.py
```

## 📦 Dependencies

The following libraries are installed:

- **delta-sharing** (>=1.0.0): Official Python client for Delta Sharing
- **pandas** (>=2.0.0): Data manipulation
- **pyarrow** (>=12.0.0): Apache Arrow format support
- **requests** (>=2.31.0): HTTP client for direct API testing
- **tabulate** (>=0.9.0): Console table formatting

## 🧪 Implemented Tests

The `test_delta_sharing.py` script executes the following tests:

### Test 1: List All Shares
Lists all shares available on the server.

```python
client = delta_sharing.SharingClient("config.share")
shares = client.list_shares()
```

### Test 2: List Schemas in Shares
Lista todos os schemas em cada share.

```python
schemas = client.list_schemas(share)
```

### Test 3: List Tables in Schemas
Lista todas as tables em cada schema.

```python
tables = client.list_tables(schema)
```

### Test 4: List All Tables
Lista todas as tables de todos os shares de uma vez.

```python
all_tables = client.list_all_tables()
```

### Test 5: Load Table Data
Carrega dados de uma table e exibe as primeiras linhas.

```python
df = delta_sharing.load_as_pandas(table_url)
```

### Test 6: Get Table Metadata
Gets the schema (columns and types) of a table.

```python
df = delta_sharing.load_as_pandas(table_url, limit=1)
schema = df.dtypes
```

### Test 7: Direct REST API Calls
Testa diretamente os endpoints REST da API.

```python
response = requests.get(url, headers={"Authorization": "Bearer test"})
```

## 📁 Files

### `config.share`
Delta Sharing configuration file containing:
- Server endpoint
- Bearer token for authentication
- Protocol version

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "http://localhost:8080/delta-sharing",
  "bearerToken": "test"
}
```

### `requirements.txt`
List of required Python dependencies.

### `test_delta_sharing.py`
Main test script with 7 different tests.

### `setup.sh`
Environment setup script.

### `run_tests.sh`
Script to run tests with pre-checks.

## 🔧 Configuration

### Change the Endpoint

Edit the `config.share` file:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "http://your-server:port/delta-sharing",
  "bearerToken": "your-token"
}
```

### Change the Token

The default token is `test`. To use another token:

1. Edit `config.share` and change the `bearerToken` field
2. Configure the environment variable on the server: `export DELTA_SHARING_TOKEN=your-token`

## 📊 Example Output

```
🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷
  Delta Sharing OnPrem - Python Client Test Suite
🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷🔷

Configuration:
  Profile: config.share
  Endpoint: http://localhost:8080/delta-sharing
  Token: test

================================================================================
  TEST 1: List All Shares
================================================================================

✅ Found 2 share(s):
  - demo-share
  - production-share

================================================================================
  TEST 2: List Schemas in Shares
================================================================================

📂 Share: demo-share
   Found 2 schema(s):
     - default
     - analytics

================================================================================
  TEST 3: List Tables in Schemas
================================================================================

📁 Schema: demo-share.default
   Found 2 table(s):
     - customers
     - orders

================================================================================
  TEST 4: List All Tables
================================================================================

✅ Found 4 table(s) total:

  📂 demo-share:
     default.customers
     default.orders
     analytics.sales

================================================================================
  TEST 5: Load Table Data
================================================================================

📋 Loading data from: demo-share.default.customers
   Table URL: config.share#demo-share.default.customers

✅ Successfully loaded table!
   Rows: 100
   Columns: 4
   Column names: id, name, email, created_at

   First 5 rows:
+------+---------------+----------------------+---------------------+
| id   | name          | email                | created_at          |
+======+===============+======================+=====================+
| 1    | John Doe      | john@example.com     | 2024-01-01 10:00:00 |
| 2    | Jane Smith    | jane@example.com     | 2024-01-02 11:30:00 |
+------+---------------+----------------------+---------------------+

================================================================================
  TEST SUMMARY
================================================================================

✅ All tests completed!
   Shares: 2
   Schemas: 3
   Tables: 4
```

## 🐛 Troubleshooting

### Error: "Connection refused"
- Check if the server is running: `curl http://localhost:8080/delta-sharing/shares -H "Authorization: Bearer test"`
- Start the server: `cd .. && ./run.sh`

### Error: "401 Unauthorized"
- Check if the token in `config.share` is correct
- Default token is `test`

### Error: "No shares found"
- Check if the database has sample data
- Run the data script: `src/main/resources/data.sql`

### Error installing dependencies
```bash
# Update pip
pip install --upgrade pip

# Install dependencies one by one
pip install delta-sharing
pip install pandas
pip install pyarrow
pip install requests
pip install tabulate
```

## 📚 Additional Documentation

- [Delta Sharing Protocol](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md)
- [Delta Sharing Python Client](https://github.com/delta-io/delta-sharing/tree/main/python)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

## 🔗 Useful Links

- **Server**: http://localhost:8080
- **Dashboard**: http://localhost:8080/
- **API Docs**: http://localhost:8080/swagger-ui.html
- **Delta Sharing API**: http://localhost:8080/delta-sharing

## 📝 Notes

- Tests assume the server is running with sample data
- Default token is `test` (configurable via environment variable)
- Tests are non-destructive (read-only)
- Use `Ctrl+C` to interrupt tests at any time

## 🤝 Contributing

To add new tests:

1. Edit `test_delta_sharing.py`
2. Add a new `test_*` function
3. Call the function in `main()`
4. Run `./run_tests.sh` to verify

---

**Developed for Delta Sharing OnPrem v1.2.0**
