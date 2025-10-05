#!/bin/bash

# Run Delta Sharing tests

echo "🔷 Delta Sharing OnPrem - Running Tests"
echo "========================================"
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run setup.sh first."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Check if server is running
echo "Checking if Delta Sharing server is running..."
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/delta-sharing/shares -H "Authorization: Bearer test" > /tmp/server_check.txt
HTTP_CODE=$(cat /tmp/server_check.txt)

if [ "$HTTP_CODE" != "200" ]; then
    echo "⚠️  Warning: Server may not be running (HTTP $HTTP_CODE)"
    echo "   Make sure the Delta Sharing OnPrem server is running on http://localhost:8080"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo "✅ Server is running"
fi

echo ""
echo "Running tests..."
echo ""

# Run the test script
python test_delta_sharing.py

# Capture exit code
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ All tests completed"
else
    echo "❌ Tests failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE
