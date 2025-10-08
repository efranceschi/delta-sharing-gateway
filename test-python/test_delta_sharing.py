#!/usr/bin/env python3
"""
Test script for Delta Sharing OnPrem server
Tests the Delta Sharing protocol implementation using the official Python client
"""

import delta_sharing
import pandas as pd
import sys
import traceback
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
    
    import requests
    
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        all_tables = client.list_all_tables()
        
        print(f"✅ Found {len(all_tables)} table(s) total:")
        
        # Prepare headers for direct API calls to get table format
        base_url = "http://localhost:8080/delta-sharing"
        headers = {
            "Authorization": "Bearer test",
            "Content-Type": "application/json"
        }
        
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
                # Get table format from metadata endpoint
                try:
                    import json
                    url = f"{base_url}/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/metadata"
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        # Parse NDJSON response
                        table_format = 'unknown'
                        for line in response.text.strip().split('\n'):
                            obj = json.loads(line)
                            if 'metaData' in obj:
                                metadata = obj['metaData']
                                if 'format' in metadata and 'provider' in metadata['format']:
                                    table_format = metadata['format']['provider']
                                    break
                    else:
                        table_format = 'unknown'
                except:
                    table_format = 'unknown'
                
                print(f"     {table.schema}.{table.name} [{table_format}]")
        
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
    """Test 6: Get table metadata and statistics"""
    print_section("TEST 6: Get Table Metadata and Statistics")
    
    if not tables:
        print("⚠️  No tables available to get metadata from")
        return
    
    # Test with the first table
    table = tables[0]
    table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
    
    print(f"📋 Getting metadata for: {table.share}.{table.schema}.{table.name}\n")
    
    try:
        # Load table to get schema and statistics
        df = delta_sharing.load_as_pandas(table_url, limit=10)
        
        print(f"✅ Table Schema:")
        schema_data = []
        for col_name, col_type in df.dtypes.items():
            schema_data.append([col_name, str(col_type)])
        
        print(tabulate(schema_data, headers=['Column', 'Type'], tablefmt='grid'))
        
        # Calculate and display statistics
        print(f"\n📊 Table Statistics:")
        
        stats_data = []
        stats_data.append(['Total Rows', len(df)])
        stats_data.append(['Total Columns', len(df.columns)])
        stats_data.append(['Memory Usage', f'{df.memory_usage(deep=True).sum() / 1024:.2f} KB'])
        
        print(tabulate(stats_data, headers=['Metric', 'Value'], tablefmt='grid'))
        
        # Column statistics
        print(f"\n📈 Column Statistics:")
        col_stats = []
        
        for col in df.columns:
            try:
                # Get dtype
                dtype = str(df[col].dtype)
                
                # Count nulls
                null_count = df[col].isnull().sum()
                null_pct = f"{(null_count / len(df) * 100):.1f}%"
                
                # Get unique values count
                unique_count = df[col].nunique()
                
                # Get min/max for numeric columns
                if df[col].dtype in ['int64', 'float64']:
                    min_val = df[col].min()
                    max_val = df[col].max()
                    mean_val = df[col].mean()
                    col_stats.append([
                        col, 
                        dtype, 
                        null_count, 
                        null_pct,
                        unique_count,
                        f"{min_val:.2f}" if pd.notna(min_val) else 'N/A',
                        f"{max_val:.2f}" if pd.notna(max_val) else 'N/A',
                        f"{mean_val:.2f}" if pd.notna(mean_val) else 'N/A'
                    ])
                else:
                    # For non-numeric columns
                    col_stats.append([
                        col, 
                        dtype, 
                        null_count, 
                        null_pct,
                        unique_count,
                        'N/A',
                        'N/A',
                        'N/A'
                    ])
            except Exception as e:
                col_stats.append([col, dtype, 'Error', 'Error', 'Error', 'Error', 'Error', 'Error'])
        
        print(tabulate(
            col_stats, 
            headers=['Column', 'Type', 'Nulls', 'Null %', 'Unique', 'Min', 'Max', 'Mean'], 
            tablefmt='grid'
        ))
        
        # Get file list and statistics from query endpoint
        print(f"\n📁 File List and Statistics:")
        
        import requests
        import json
        
        base_url = "http://localhost:8080/delta-sharing"
        headers = {
            "Authorization": "Bearer test",
            "Content-Type": "application/json"
        }
        
        try:
            # Query the table to get file information
            query_url = f"{base_url}/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/query"
            response = requests.post(query_url, headers=headers, json={"limitHint": 10})
            
            if response.status_code == 200:
                files_data = []
                
                # Parse NDJSON response
                for line in response.text.strip().split('\n'):
                    obj = json.loads(line)
                    
                    # Look for file entries
                    if 'file' in obj:
                        file_info = obj['file']
                        
                        # Extract URL (truncate for readability)
                        url = file_info.get('url', 'N/A')
                        url_display = url if len(url) <= 60 else url[:57] + '...'
                        
                        # Extract file statistics
                        stats = file_info.get('stats', {})
                        if isinstance(stats, dict):
                            num_records = stats.get('numRecords', 'N/A')
                            null_count = stats.get('nullCount', {})
                            total_nulls = sum(null_count.values()) if isinstance(null_count, dict) else 'N/A'
                        else:
                            num_records = 'N/A'
                            total_nulls = 'N/A'
                        
                        # Extract other file properties
                        file_size = file_info.get('size', 0)
                        file_size_display = f"{file_size / 1024:.2f} KB" if file_size > 0 else 'N/A'
                        
                        partition_values = file_info.get('partitionValues', {})
                        partition_display = ', '.join([f"{k}={v}" for k, v in partition_values.items()]) if partition_values else 'None'
                        if len(partition_display) > 30:
                            partition_display = partition_display[:27] + '...'
                        
                        files_data.append([
                            url_display,
                            file_size_display,
                            num_records,
                            total_nulls,
                            partition_display
                        ])
                
                if files_data:
                    print(tabulate(
                        files_data,
                        headers=['File URL', 'Size', 'Records', 'Total Nulls', 'Partitions'],
                        tablefmt='grid'
                    ))
                    print(f"\n   Total files: {len(files_data)}")
                else:
                    print("   No file information available in query response")
            else:
                print(f"   ⚠️  Could not retrieve file list (HTTP {response.status_code})")
        
        except Exception as file_err:
            print(f"   ⚠️  Error retrieving file list: {file_err}")
        
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
    
    # Dynamically discover first share and schema
    try:
        shares_response = requests.get(f"{base_url}/shares", headers=headers)
        first_share = shares_response.json()['items'][0]['name'] if shares_response.status_code == 200 else 'analytics-share'
        
        schemas_response = requests.get(f"{base_url}/shares/{first_share}/schemas", headers=headers)
        first_schema = schemas_response.json()['items'][0]['name'] if schemas_response.status_code == 200 else 'default'
        
        tables_response = requests.get(f"{base_url}/shares/{first_share}/schemas/{first_schema}/tables", headers=headers)
        first_table = tables_response.json()['items'][0]['name'] if tables_response.status_code == 200 else 'test'
    except:
        first_share = 'analytics-share'
        first_schema = 'default'
        first_table = 'test'
    
    tests = [
        ("GET /shares", f"{base_url}/shares", "GET"),
        (f"GET /shares/{first_share}", f"{base_url}/shares/{first_share}", "GET"),
        (f"GET /shares/{first_share}/schemas", f"{base_url}/shares/{first_share}/schemas", "GET"),
        (f"GET /shares/{first_share}/schemas/{first_schema}/tables", 
         f"{base_url}/shares/{first_share}/schemas/{first_schema}/tables", "GET"),
        (f"GET /shares/{first_share}/all-tables", 
         f"{base_url}/shares/{first_share}/all-tables", "GET"),
        (f"GET /shares/{first_share}/schemas/{first_schema}/tables/{first_table}/version",
         f"{base_url}/shares/{first_share}/schemas/{first_schema}/tables/{first_table}/version", "GET"),
        (f"GET /shares/{first_share}/schemas/{first_schema}/tables/{first_table}/metadata",
         f"{base_url}/shares/{first_share}/schemas/{first_schema}/tables/{first_table}/metadata", "GET"),
        (f"POST /shares/{first_share}/schemas/{first_schema}/tables/{first_table}/query",
         f"{base_url}/shares/{first_share}/schemas/{first_schema}/tables/{first_table}/query", "POST"),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, url, method in tests:
        try:
            if method == "GET":
                response = requests.get(url, headers=headers)
            else:  # POST
                response = requests.post(url, headers=headers, json={"limitHint": 1})
            
            if response.status_code == 200:
                print(f"✅ {test_name}")
                passed += 1
            else:
                print(f"❌ {test_name} (HTTP {response.status_code})")
                failed += 1
        except Exception as e:
            print(f"❌ {test_name} (Exception: {type(e).__name__})")
            failed += 1
    
    print(f"\n📊 Results: {passed} passed, {failed} failed out of {len(tests)} tests")

def test_data_skipping():
    """Test 8: Data Skipping with Predicates"""
    print_section("TEST 8: Data Skipping with Predicates")
    
    import requests
    import json
    from collections import defaultdict
    
    base_url = "http://localhost:8080/delta-sharing"
    headers = {
        "Authorization": "Bearer test",
        "Content-Type": "application/json"
    }
    
    # Dynamically discover resources
    print("📋 Discovering resources...")
    try:
        shares_response = requests.get(f"{base_url}/shares", headers=headers)
        share = shares_response.json()['items'][0]['name'] if shares_response.status_code == 200 else 'analytics-share'
        
        schemas_response = requests.get(f"{base_url}/shares/{share}/schemas", headers=headers)
        schema = schemas_response.json()['items'][0]['name'] if schemas_response.status_code == 200 else 'default'
        
        tables_response = requests.get(f"{base_url}/shares/{share}/schemas/{schema}/tables", headers=headers)
        table = tables_response.json()['items'][0]['name'] if tables_response.status_code == 200 else 'test'
        
        print(f"✅ Share: {share}")
        print(f"✅ Schema: {schema}")
        print(f"✅ Table: {table}\n")
    except Exception as e:
        print(f"❌ Error discovering resources: {e}")
        return
    
    query_url = f"{base_url}/shares/{share}/schemas/{schema}/tables/{table}/query"
    
    # Helper function to parse files from NDJSON response
    def parse_files(response_text):
        files = []
        for line in response_text.strip().split('\n'):
            try:
                obj = json.loads(line)
                if 'file' in obj:
                    files.append(obj['file'])
            except:
                pass
        return files
    
    # Helper function to display partition summary
    def show_partitions(files):
        if not files:
            return
        
        partitions_set = set()
        for f in files:
            part_vals = f.get('partitionValues', {})
            if part_vals:
                part_str = ', '.join([f"{k}={v}" for k, v in sorted(part_vals.items())])
                partitions_set.add(part_str)
        
        if partitions_set:
            print("\n  Partitions found:")
            for part in sorted(partitions_set):
                count = sum(1 for f in files 
                           if ', '.join([f"{k}={v}" for k, v in sorted(f.get('partitionValues', {}).items())]) == part)
                print(f"    - {part}: {count} file(s)")
    
    # Test 8.1: Baseline (no predicates) - Discover partitions
    print("─" * 70)
    print("TEST 8.1: Query without predicates (baseline)")
    print("─" * 70)
    
    baseline_files = []
    partition_columns = []
    partition_values_by_column = defaultdict(set)
    
    try:
        response = requests.post(query_url, headers=headers, json={"limitHint": 100})
        baseline_files = parse_files(response.text)
        
        print(f"\n📊 Result:")
        print(f"  Total files returned: {len(baseline_files)}")
        
        # Discover partition columns and values
        for f in baseline_files:
            part_vals = f.get('partitionValues', {})
            if part_vals:
                for col, val in part_vals.items():
                    partition_values_by_column[col].add(val)
        
        partition_columns = sorted(partition_values_by_column.keys())
        
        if partition_columns:
            print(f"\n  Discovered partition columns: {', '.join(partition_columns)}")
            for col in partition_columns:
                values = sorted(partition_values_by_column[col])
                print(f"    - {col}: {', '.join(values[:5])}{' ...' if len(values) > 5 else ''}")
        else:
            print(f"\n  ⚠️  No partition columns found (table not partitioned)")
        
        show_partitions(baseline_files)
        
        baseline_count = len(baseline_files)
    except Exception as e:
        print(f"❌ Error: {e}")
        baseline_count = 0
        print("\n⚠️  Cannot continue with data skipping tests without baseline data")
        return
    
    if not partition_columns or baseline_count == 0:
        print("\n⚠️  Cannot test data skipping: table has no partitions or no files")
        return
    
    print()
    
    # Test 8.2: Single predicate using discovered partition values
    print("─" * 70)
    first_col = partition_columns[0]
    first_val = sorted(partition_values_by_column[first_col])[0]
    print(f"TEST 8.2: Query with single predicate ({first_col} = {first_val})")
    print("─" * 70)
    
    try:
        predicate = f"{first_col} = {first_val}"
        response = requests.post(query_url, headers=headers, 
                                json={"predicateHints": [predicate], "limitHint": 100})
        files = parse_files(response.text)
        
        print(f"\n📊 Result:")
        print(f"  Total files returned: {len(files)}")
        
        # Validate all files have the expected partition value
        invalid_files = [f for f in files if f.get('partitionValues', {}).get(first_col) != first_val]
        
        if invalid_files:
            print(f"\n  ❌ FAIL: {len(invalid_files)} file(s) don't have {first_col}={first_val}:")
            for f in invalid_files[:3]:  # Show max 3
                print(f"    - {f.get('id')}: {f.get('partitionValues')}")
        else:
            if len(files) > 0:
                print(f"\n  ✅ SUCCESS: All files have {first_col}={first_val}")
            else:
                print(f"\n  ⚠️  No files matched predicate (possible bug)")
        
        show_partitions(files)
        
        if baseline_count > 0:
            reduction = ((baseline_count - len(files)) / baseline_count * 100) if len(files) < baseline_count else 0
            print(f"\n  ⚡ Data skipping: {baseline_count} → {len(files)} files ({reduction:.1f}% reduction)")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print()
    
    # Test 8.3: Multiple predicates using discovered values
    print("─" * 70)
    if len(partition_columns) >= 2:
        second_col = partition_columns[1]
        second_val = sorted(partition_values_by_column[second_col])[0]
        print(f"TEST 8.3: Query with multiple predicates ({first_col} = {first_val} AND {second_col} = {second_val})")
    else:
        # Use same column with different value if only one partition column
        second_val = sorted(partition_values_by_column[first_col])[0] if len(partition_values_by_column[first_col]) > 0 else first_val
        second_col = first_col
        print(f"TEST 8.3: Query with multiple predicates ({first_col} = {first_val} AND {second_col} = {second_val})")
    print("─" * 70)
    
    try:
        predicates = [f"{first_col} = {first_val}", f"{second_col} = {second_val}"]
        response = requests.post(query_url, headers=headers, 
                                json={"predicateHints": predicates, "limitHint": 100})
        files = parse_files(response.text)
        
        print(f"\n📊 Result:")
        print(f"  Total files returned: {len(files)}")
        
        # Validate all files match both predicates
        invalid_files = [f for f in files 
                        if f.get('partitionValues', {}).get(first_col) != first_val 
                        or f.get('partitionValues', {}).get(second_col) != second_val]
        
        if invalid_files:
            print(f"\n  ❌ FAIL: {len(invalid_files)} file(s) don't match predicates:")
            for f in invalid_files[:3]:  # Show max 3
                print(f"    - {f.get('id')}: {f.get('partitionValues')}")
        else:
            if len(files) > 0:
                print(f"\n  ✅ SUCCESS: All files match predicates")
            else:
                print(f"\n  ⚠️  No files matched predicates (expected if values don't coexist)")
        
        show_partitions(files)
        
        if baseline_count > 0:
            reduction = ((baseline_count - len(files)) / baseline_count * 100) if len(files) < baseline_count else 0
            print(f"\n  ⚡ Data skipping: {baseline_count} → {len(files)} files ({reduction:.1f}% reduction)")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print()
    
    # Test 8.4: limitHint validation
    print("─" * 70)
    print("TEST 8.4: Query with limitHint = 2")
    print("─" * 70)
    
    try:
        response = requests.post(query_url, headers=headers, json={"limitHint": 2})
        files = parse_files(response.text)
        
        print(f"\n📊 Result:")
        print(f"  Total files returned: {len(files)}")
        
        if len(files) <= 2:
            print(f"\n  ✅ SUCCESS: limitHint respected (max 2 files)")
        else:
            print(f"\n  ❌ FAIL: limitHint not respected (expected <= 2, got {len(files)})")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "─" * 70)
    print("✨ DATA SKIPPING TESTS COMPLETED")
    print("─" * 70)

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
        
        # Test 8: Data skipping
        test_data_skipping()
        
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
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
