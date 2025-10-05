#!/usr/bin/env python3
"""
Simple example of using Delta Sharing Python client
Demonstrates basic usage patterns
"""

import delta_sharing
import pandas as pd

# Path to the profile file
PROFILE_FILE = "config.share"

def example_1_list_resources():
    """Example 1: List all available resources"""
    print("="*60)
    print("Example 1: Listing Resources")
    print("="*60)
    
    # Create a SharingClient
    client = delta_sharing.SharingClient(PROFILE_FILE)
    
    # List all shares
    print("\n📂 Available Shares:")
    shares = client.list_shares()
    for share in shares:
        print(f"  - {share.name}")
    
    # List all tables
    print("\n📋 Available Tables:")
    tables = client.list_all_tables()
    for table in tables:
        print(f"  - {table.share}.{table.schema}.{table.name}")

def example_2_load_table():
    """Example 2: Load a table as Pandas DataFrame"""
    print("\n" + "="*60)
    print("Example 2: Loading Table Data")
    print("="*60)
    
    # Define the table URL
    # Format: <profile>#<share>.<schema>.<table>
    table_url = f"{PROFILE_FILE}#demo-share.default.customers"
    
    print(f"\nLoading table: {table_url}")
    
    # Load the table
    df = delta_sharing.load_as_pandas(table_url)
    
    print(f"\n✅ Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"\nColumns: {', '.join(df.columns.tolist())}")
    print(f"\nFirst 3 rows:")
    print(df.head(3))

def example_3_load_with_limit():
    """Example 3: Load table with row limit"""
    print("\n" + "="*60)
    print("Example 3: Loading with Limit")
    print("="*60)
    
    table_url = f"{PROFILE_FILE}#demo-share.default.customers"
    
    # Load only first 10 rows
    df = delta_sharing.load_as_pandas(table_url, limit=10)
    
    print(f"\n✅ Loaded {len(df)} rows (limited to 10)")
    print(df)

def example_4_filter_data():
    """Example 4: Filter and analyze data"""
    print("\n" + "="*60)
    print("Example 4: Filtering and Analysis")
    print("="*60)
    
    table_url = f"{PROFILE_FILE}#demo-share.default.customers"
    
    # Load the data
    df = delta_sharing.load_as_pandas(table_url)
    
    # Perform some analysis
    print(f"\n📊 Data Analysis:")
    print(f"  Total rows: {len(df)}")
    print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    # Show data types
    print(f"\n📋 Column Types:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")

def example_5_iterate_all_tables():
    """Example 5: Iterate through all tables"""
    print("\n" + "="*60)
    print("Example 5: Iterating All Tables")
    print("="*60)
    
    client = delta_sharing.SharingClient(PROFILE_FILE)
    tables = client.list_all_tables()
    
    print(f"\nFound {len(tables)} tables\n")
    
    for table in tables:
        table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
        
        try:
            # Load with limit to avoid loading too much data
            df = delta_sharing.load_as_pandas(table_url, limit=5)
            print(f"✅ {table.share}.{table.schema}.{table.name}")
            print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
            print(f"   Columns: {', '.join(df.columns.tolist())}")
        except Exception as e:
            print(f"❌ {table.share}.{table.schema}.{table.name}: {e}")
        
        print()

def main():
    """Run all examples"""
    print("\n" + "🔷"*30)
    print("  Delta Sharing Python Client - Usage Examples")
    print("🔷"*30)
    
    try:
        # Run examples
        example_1_list_resources()
        example_2_load_table()
        example_3_load_with_limit()
        example_4_filter_data()
        example_5_iterate_all_tables()
        
        print("\n" + "="*60)
        print("✅ All examples completed successfully!")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
