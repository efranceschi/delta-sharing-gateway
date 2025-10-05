# Quick Start - Python Testing

## 🚀 Quick Start (3 steps)

### 1️⃣ Setup (one time only)

```bash
cd test-python
./setup.sh
```

### 2️⃣ Start the server (in another terminal)

```bash
cd ..
./run.sh
```

### 3️⃣ Run the tests

```bash
./run_tests.sh
```

## 📝 Useful Commands

### Run complete test suite
```bash
source venv/bin/activate
python test_delta_sharing.py
```

### Run usage examples
```bash
source venv/bin/activate
python example_usage.py
```

### Test manually with Python
```bash
source venv/bin/activate
python
```

```python
import delta_sharing

# Create client
client = delta_sharing.SharingClient("config.share")

# List shares
shares = client.list_shares()
for share in shares:
    print(share.name)

# Load a table
df = delta_sharing.load_as_pandas("config.share#demo-share.default.customers")
print(df.head())
```

## 🔧 Configuration

Edit `config.share` to change endpoint or token:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "http://localhost:8080/delta-sharing",
  "bearerToken": "test"
}
```

## ✅ Check if it's working

```bash
# Test connection to the server
curl -H "Authorization: Bearer test" \
     http://localhost:8080/delta-sharing/shares

# Should return JSON with list of shares
```

## 🐛 Common Issues

### "Connection refused"
→ Server is not running. Run `../run.sh`

### "401 Unauthorized"
→ Incorrect token. Check `config.share` and `DELTA_SHARING_TOKEN` variable

### "No module named 'delta_sharing'"
→ Virtual environment not activated. Run `source venv/bin/activate`

## 📚 More Information

- [README.md](README.md) - Complete documentation
- [../DELTA_SHARING_README.md](../DELTA_SHARING_README.md) - Delta Sharing Protocol
- [../TESTING.md](../TESTING.md) - General testing guide