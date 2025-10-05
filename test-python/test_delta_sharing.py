#!/usr/bin/env python3
"""
Test script for Delta Sharing OnPrem server
Tests the Delta Sharing protocol implementation using the official Python client
"""

import delta_sharing
import pandas as pd
import sys
from tabulate import tabulate

# Configuration
PROFILE_FILE = "config.share"

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

def test_list_shares():
    """Test 1: List all shares"""
    print_section("TEST 1: List All Shares")
    
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        shares = client.list_shares()
        
        print(f"✅ Found {len(shares)} share(s):")
        for share in shares:
            print(f"  - {share.name}")
        
        return shares
    except Exception as e:
        print(f"❌ Error listing shares: {e}")
        return []

def test_list_schemas(shares):
    """Test 2: List schemas in each share"""
    print_section("TEST 2: List Schemas in Shares")
    
    all_schemas = []
    
    for share in shares:
        try:
            client = delta_sharing.SharingClient(PROFILE_FILE)
            schemas = client.list_schemas(share)
            
            print(f"📂 Share: {share.name}")
            print(f"   Found {len(schemas)} schema(s):")
            for schema in schemas:
                print(f"     - {schema.name}")
                all_schemas.append((share, schema))
        except Exception as e:
            print(f"❌ Error listing schemas for {share.name}: {e}")
    
    return all_schemas

def test_list_tables(share_schemas):
    """Test 3: List tables in each schema"""
    print_section("TEST 3: List Tables in Schemas")
    
    all_tables = []
    
    for share, schema in share_schemas:
        try:
            client = delta_sharing.SharingClient(PROFILE_FILE)
            tables = client.list_tables(schema)
            
            print(f"📁 Schema: {share.name}.{schema.name}")
            print(f"   Found {len(tables)} table(s):")
            for table in tables:
                print(f"     - {table.name}")
                all_tables.append(table)
        except Exception as e:
            print(f"❌ Error listing tables for {share.name}.{schema.name}: {e}")
    
    return all_tables

def test_list_all_tables():
    """Test 4: List all tables across all shares"""
    print_section("TEST 4: List All Tables")
    
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        all_tables = client.list_all_tables()
        
        print(f"✅ Found {len(all_tables)} table(s) total:")
        
        # Group by share
        tables_by_share = {}
        for table in all_tables:
            share_name = table.share
            if share_name not in tables_by_share:
                tables_by_share[share_name] = []
            tables_by_share[share_name].append(table)
        
        for share_name, tables in tables_by_share.items():
            print(f"\n  📂 {share_name}:")
            for table in tables:
                print(f"     {table.schema}.{table.name}")
        
        return all_tables
    except Exception as e:
        print(f"❌ Error listing all tables: {e}")
        return []

def test_load_table_data(tables):
    """Test 5: Load data from tables"""
    print_section("TEST 5: Load Table Data")
    
    if not tables:
        print("⚠️  No tables available to load data from")
        return
    
    # Test with the first table
    table = tables[0]
    table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
    
    print(f"📋 Loading data from: {table.share}.{table.schema}.{table.name}")
    print(f"   Table URL: {table_url}\n")
    
    try:
        # Load as Pandas DataFrame
        df = delta_sharing.load_as_pandas(table_url)
        
        print(f"✅ Successfully loaded table!")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {len(df.columns)}")
        print(f"   Column names: {', '.join(df.columns.tolist())}")
        
        # Show first few rows
        if len(df) > 0:
            print(f"\n   First {min(5, len(df))} rows:")
            print(tabulate(df.head(), headers='keys', tablefmt='grid', showindex=False))
        
        return df
    except Exception as e:
        print(f"❌ Error loading table data: {e}")
        return None

def test_table_metadata(tables):
    """Test 6: Get table metadata"""
    print_section("TEST 6: Get Table Metadata")
    
    if not tables:
        print("⚠️  No tables available to get metadata from")
        return
    
    # Test with the first table
    table = tables[0]
    table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
    
    print(f"📋 Getting metadata for: {table.share}.{table.schema}.{table.name}\n")
    
    try:
        # Get table version
        client = delta_sharing.SharingClient(PROFILE_FILE)
        
        # Load table to get schema info
        df = delta_sharing.load_as_pandas(table_url, limit=1)
        
        print(f"✅ Table Schema:")
        schema_data = []
        for col_name, col_type in df.dtypes.items():
            schema_data.append([col_name, str(col_type)])
        
        print(tabulate(schema_data, headers=['Column', 'Type'], tablefmt='grid'))
        
    except Exception as e:
        print(f"❌ Error getting table metadata: {e}")

def test_direct_api():
    """Test 7: Direct API calls"""
    print_section("TEST 7: Direct REST API Calls")
    
    import requests
    
    base_url = "http://localhost:8080/delta-sharing"
    headers = {
        "Authorization": "Bearer test",
        "Content-Type": "application/json"
    }
    
    tests = [
        ("GET /shares", f"{base_url}/shares"),
        ("GET /shares/demo-share", f"{base_url}/shares/demo-share"),
        ("GET /shares/demo-share/schemas", f"{base_url}/shares/demo-share/schemas"),
    ]
    
    for test_name, url in tests:
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                print(f"✅ {test_name}: OK (200)")
                data = response.json()
                if 'items' in data:
                    print(f"   Items: {len(data['items'])}")
            else:
                print(f"❌ {test_name}: Failed ({response.status_code})")
        except Exception as e:
            print(f"❌ {test_name}: Error - {e}")

def main():
    """Main test runner"""
    print("\n" + "🔷"*40)
    print("  Delta Sharing OnPrem - Python Client Test Suite")
    print("🔷"*40)
    
    print(f"\nConfiguration:")
    print(f"  Profile: {PROFILE_FILE}")
    print(f"  Endpoint: http://localhost:8080/delta-sharing")
    print(f"  Token: test")
    
    try:
        # Test 1: List shares
        shares = test_list_shares()
        
        if not shares:
            print("\n⚠️  No shares found. Make sure the server is running with sample data.")
            return
        
        # Test 2: List schemas
        share_schemas = test_list_schemas(shares)
        
        # Test 3: List tables in schemas
        tables = test_list_tables(share_schemas)
        
        # Test 4: List all tables
        all_tables = test_list_all_tables()
        
        # Test 5: Load table data
        if all_tables:
            test_load_table_data(all_tables)
        
        # Test 6: Get table metadata
        if all_tables:
            test_table_metadata(all_tables)
        
        # Test 7: Direct API calls
        test_direct_api()
        
        # Summary
        print_section("TEST SUMMARY")
        print(f"✅ All tests completed!")
        print(f"   Shares: {len(shares)}")
        print(f"   Schemas: {len(share_schemas)}")
        print(f"   Tables: {len(all_tables)}")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
