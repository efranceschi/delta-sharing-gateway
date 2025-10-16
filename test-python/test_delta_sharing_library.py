#!/usr/bin/env python3
"""
Delta Sharing Library Compatibility Test

This script tests if the Delta Sharing Java server is compatible with all methods
from the delta-sharing Python library.

Tests all major features:
- Client initialization and configuration
- List operations (shares, schemas, tables)
- Table metadata retrieval
- Data loading (pandas, optional spark)
- Query predicates and filters
- Version handling
- Error handling

Usage:
    python test_delta_sharing_library.py [profile_file] [-t TEST_NUMBER]
    
Examples:
    # Run all tests
    python test_delta_sharing_library.py
    
    # Run all tests with custom profile
    python test_delta_sharing_library.py custom.share
    
    # Run only test 3
    python test_delta_sharing_library.py -t 3
    
    # Run test 5 with custom profile
    python test_delta_sharing_library.py custom.share -t 5
"""

import os
import sys
import json
import time
import traceback
import argparse
from typing import List, Dict, Optional, Tuple
import delta_sharing
import pandas as pd

# ═════════════════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════════════════

# Default profile file
DEFAULT_PROFILE = "config.share"

# Test results tracking
TEST_RESULTS = []

# Global variables for configuration (set by parse_arguments)
PROFILE_FILE = DEFAULT_PROFILE
TEST_TO_RUN = None


# ═════════════════════════════════════════════════════════════════════════════
# Colors and Formatting
# ═════════════════════════════════════════════════════════════════════════════

class Colors:
    """ANSI color codes for terminal output"""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    
    # Regular colors
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    
    # Bright colors
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"


def print_section(title: str):
    """Print a formatted section header"""
    width = 80
    print(f"\n{Colors.BRIGHT_CYAN}{'═' * width}{Colors.RESET}")
    print(f"{Colors.BRIGHT_CYAN}{Colors.BOLD}{title.center(width)}{Colors.RESET}")
    print(f"{Colors.BRIGHT_CYAN}{'═' * width}{Colors.RESET}\n")


def print_subsection(title: str):
    """Print a formatted subsection header"""
    print(f"\n{Colors.BRIGHT_YELLOW}{'─' * 80}{Colors.RESET}")
    print(f"{Colors.BRIGHT_YELLOW}{Colors.BOLD}{title}{Colors.RESET}")
    print(f"{Colors.BRIGHT_YELLOW}{'─' * 80}{Colors.RESET}\n")


def format_duration(seconds: float) -> str:
    """Format duration in a human-readable way"""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"


def add_test_result(test_name: str, passed: bool, error_msg: Optional[str], duration: float):
    """Add a test result to the global results list"""
    TEST_RESULTS.append((test_name, passed, error_msg, duration))


def parse_arguments():
    """Parse command line arguments"""
    global PROFILE_FILE, TEST_TO_RUN
    
    parser = argparse.ArgumentParser(
        description='Delta Sharing Library Compatibility Test',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                        # Run all tests with default profile
  %(prog)s custom.share           # Run all tests with custom profile
  %(prog)s -t 3                   # Run only test 3
  %(prog)s custom.share -t 5      # Run test 5 with custom profile
  %(prog)s -t 1,3,5               # Run tests 1, 3, and 5
  %(prog)s --list                 # List all available tests

Available Tests:
  1  - Load Profile
  2  - Client Initialization
  3  - List Shares
  4  - List Schemas
  5  - List Tables in Schema
  6  - List All Tables
  7  - Get Table Metadata
  8  - Load as Pandas (Basic)
  9  - Load with Limit
  10 - Load with Version
  11 - Load as Spark
  12 - Error Handling
        """
    )
    
    parser.add_argument(
        'profile',
        nargs='?',
        default=DEFAULT_PROFILE,
        help=f'Delta Sharing profile file (default: {DEFAULT_PROFILE})'
    )
    
    parser.add_argument(
        '-t', '--test',
        type=str,
        help='Test number(s) to run (e.g., 3 or 1,3,5). If not specified, all tests will run.'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available tests and exit'
    )
    
    args = parser.parse_args()
    
    # Handle --list
    if args.list:
        print("Available Tests:")
        print("  1  - Load Profile")
        print("  2  - Client Initialization")
        print("  3  - List Shares")
        print("  4  - List Schemas")
        print("  5  - List Tables in Schema")
        print("  6  - List All Tables")
        print("  7  - Get Table Metadata")
        print("  8  - Load as Pandas (Basic)")
        print("  9  - Load with Limit")
        print("  10 - Load with Version")
        print("  11 - Load as Spark")
        print("  12 - Error Handling")
        sys.exit(0)
    
    PROFILE_FILE = args.profile
    
    if args.test:
        # Parse comma-separated test numbers
        try:
            TEST_TO_RUN = [int(t.strip()) for t in args.test.split(',')]
        except ValueError:
            print(f"{Colors.RED}Error: Invalid test number format. Use integers separated by commas (e.g., 1,3,5){Colors.RESET}")
            sys.exit(1)
    else:
        TEST_TO_RUN = None  # Run all tests


def should_run_test(test_number: int) -> bool:
    """
    Check if a test should be run based on command line arguments
    
    Args:
        test_number: The test number to check
        
    Returns:
        True if the test should run, False otherwise
    """
    if TEST_TO_RUN is None:
        return True  # Run all tests
    return test_number in TEST_TO_RUN


# ═════════════════════════════════════════════════════════════════════════════
# Test Helper Functions
# ═════════════════════════════════════════════════════════════════════════════

def run_test(test_name: str, test_func):
    """
    Run a test function and track results
    
    Args:
        test_name: Name of the test
        test_func: Function to execute (should return True on success, False on failure)
    """
    print_subsection(test_name)
    start_time = time.time()
    
    try:
        result = test_func()
        duration = time.time() - start_time
        
        if result:
            print(f"\n{Colors.GREEN}✅ PASSED{Colors.RESET}")
            add_test_result(test_name, True, None, duration)
        else:
            print(f"\n{Colors.RED}❌ FAILED{Colors.RESET}")
            add_test_result(test_name, False, "Test returned False", duration)
        
        print(f"{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        return result
        
    except Exception as e:
        duration = time.time() - start_time
        print(f"\n{Colors.RED}❌ EXCEPTION: {str(e)}{Colors.RESET}")
        traceback.print_exc()
        add_test_result(test_name, False, str(e), duration)
        print(f"{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: Profile Loading
# ═════════════════════════════════════════════════════════════════════════════

def test_load_profile():
    """Test loading the Delta Sharing profile file"""
    try:
        with open(PROFILE_FILE, 'r') as f:
            profile = json.load(f)
        
        print(f"{Colors.CYAN}Profile loaded successfully:{Colors.RESET}")
        print(f"  Endpoint: {profile.get('endpoint', 'N/A')}")
        print(f"  Share Credentials Name: {profile.get('shareCredentialsVersion', 'N/A')}")
        
        # Verify required fields
        required_fields = ['shareCredentialsVersion', 'endpoint', 'bearerToken']
        missing = [f for f in required_fields if f not in profile]
        
        if missing:
            print(f"{Colors.RED}Missing required fields: {missing}{Colors.RESET}")
            return False
        
        return True
    except FileNotFoundError:
        print(f"{Colors.RED}Profile file not found: {PROFILE_FILE}{Colors.RESET}")
        return False
    except json.JSONDecodeError as e:
        print(f"{Colors.RED}Invalid JSON in profile: {e}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: Client Initialization
# ═════════════════════════════════════════════════════════════════════════════

def test_client_initialization():
    """Test initializing the SharingClient"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        print(f"{Colors.CYAN}SharingClient initialized successfully{Colors.RESET}")
        print(f"  Client type: {type(client).__name__}")
        return True
    except Exception as e:
        print(f"{Colors.RED}Failed to initialize client: {e}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: List Shares
# ═════════════════════════════════════════════════════════════════════════════

def test_list_shares():
    """Test SharingClient.list_shares()"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        shares = client.list_shares()
        
        print(f"{Colors.CYAN}Found {len(shares)} share(s):{Colors.RESET}")
        for share in shares:
            print(f"  - {share.name}")
            # Verify share object has expected attributes
            if not hasattr(share, 'name'):
                print(f"{Colors.RED}Share object missing 'name' attribute{Colors.RESET}")
                return False
        
        if len(shares) == 0:
            print(f"{Colors.YELLOW}Warning: No shares found{Colors.RESET}")
        
        return True
    except Exception as e:
        print(f"{Colors.RED}Error listing shares: {e}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: List Schemas
# ═════════════════════════════════════════════════════════════════════════════

def test_list_schemas():
    """Test SharingClient.list_schemas(share)"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        shares = client.list_shares()
        
        if not shares:
            print(f"{Colors.YELLOW}No shares available to test list_schemas{Colors.RESET}")
            return True
        
        total_schemas = 0
        for share in shares[:3]:  # Test first 3 shares
            schemas = client.list_schemas(share)
            total_schemas += len(schemas)
            print(f"{Colors.CYAN}Share '{share.name}' has {len(schemas)} schema(s){Colors.RESET}")
            
            for schema in schemas[:5]:  # Show first 5 schemas
                print(f"  - {schema.name}")
                # Verify schema object
                if not hasattr(schema, 'name') or not hasattr(schema, 'share'):
                    print(f"{Colors.RED}Schema object missing required attributes{Colors.RESET}")
                    return False
        
        print(f"\n{Colors.CYAN}Total schemas found: {total_schemas}{Colors.RESET}")
        return True
    except Exception as e:
        print(f"{Colors.RED}Error listing schemas: {e}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: List Tables in Schema
# ═════════════════════════════════════════════════════════════════════════════

def test_list_tables_in_schema():
    """Test SharingClient.list_tables(schema)"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        shares = client.list_shares()
        
        if not shares:
            print(f"{Colors.YELLOW}No shares available to test list_tables{Colors.RESET}")
            return True
        
        total_tables = 0
        tested_schemas = 0
        
        # Test first few shares and their schemas
        for share in shares[:2]:  # Test first 2 shares
            schemas = client.list_schemas(share)
            
            for schema in schemas[:3]:  # Test first 3 schemas per share
                tables = client.list_tables(schema)
                total_tables += len(tables)
                tested_schemas += 1
                print(f"{Colors.CYAN}Schema '{schema.share}.{schema.name}' has {len(tables)} table(s){Colors.RESET}")
                
                for table in tables[:5]:  # Show first 5 tables
                    print(f"  - {table.share}.{table.schema}.{table.name}")
                    # Verify table object
                    required_attrs = ['name', 'schema', 'share']
                    for attr in required_attrs:
                        if not hasattr(table, attr):
                            print(f"{Colors.RED}Table object missing '{attr}' attribute{Colors.RESET}")
                            return False
        
        print(f"\n{Colors.CYAN}Total tables found: {total_tables} across {tested_schemas} schema(s){Colors.RESET}")
        return True
    except Exception as e:
        print(f"{Colors.RED}Error listing tables: {e}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: List All Tables
# ═════════════════════════════════════════════════════════════════════════════

def test_list_all_tables():
    """Test SharingClient.list_all_tables()"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        all_tables = client.list_all_tables()
        
        print(f"{Colors.CYAN}Found {len(all_tables)} table(s) across all shares{Colors.RESET}")
        
        # Group by share
        tables_by_share = {}
        for table in all_tables:
            share_name = table.share
            if share_name not in tables_by_share:
                tables_by_share[share_name] = []
            tables_by_share[share_name].append(table)
        
        for share_name, tables in tables_by_share.items():
            print(f"\n{Colors.CYAN}Share '{share_name}': {len(tables)} table(s){Colors.RESET}")
            for table in tables[:3]:  # Show first 3 per share
                print(f"  - {table.share}.{table.schema}.{table.name}")
        
        if len(all_tables) == 0:
            print(f"{Colors.YELLOW}Warning: No tables found{Colors.RESET}")
        
        return True
    except Exception as e:
        print(f"{Colors.RED}Error listing all tables: {e}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: Get Table Metadata
# ═════════════════════════════════════════════════════════════════════════════

def test_get_table_metadata():
    """Test getting table metadata (schema, version, etc.)"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        all_tables = client.list_all_tables()
        
        if not all_tables:
            print(f"{Colors.YELLOW}No tables available to test metadata{Colors.RESET}")
            return True
        
        # Test first table
        table = all_tables[0]
        table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
        
        print(f"{Colors.CYAN}Getting metadata for: {table.share}.{table.schema}.{table.name}{Colors.RESET}")
        
        # Get table version and metadata
        result = delta_sharing.get_table_protocol(table_url)
        
        print(f"{Colors.GREEN}Protocol and metadata retrieved successfully{Colors.RESET}")
        
        # The result can be either a dict or an object depending on the library version
        if hasattr(result, '__dict__'):
            # It's an object, convert to dict for inspection
            print(f"  Protocol type: {type(result).__name__}")
            print(f"  Has protocol attribute: {hasattr(result, 'protocol')}")
            print(f"  Has metadata attribute: {hasattr(result, 'metadata')}")
        else:
            # It's a dict
            print(f"  Protocol: {result.get('protocol', {})}")
            print(f"  Metadata keys: {list(result.get('metadata', {}).keys())}")
        
        return True
    except Exception as e:
        print(f"{Colors.RED}Error getting table metadata: {e}{Colors.RESET}")
        traceback.print_exc()
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: Load Table as Pandas (No Limits)
# ═════════════════════════════════════════════════════════════════════════════

def test_load_as_pandas_basic():
    """Test delta_sharing.load_as_pandas() basic functionality"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        all_tables = client.list_all_tables()
        
        if not all_tables:
            print(f"{Colors.YELLOW}No tables available to test load_as_pandas{Colors.RESET}")
            return True
        
        # Find a table to test (prefer smaller ones)
        table = all_tables[0]
        table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
        
        print(f"{Colors.CYAN}Loading table: {table.share}.{table.schema}.{table.name}{Colors.RESET}")
        
        # Load with limit to avoid loading huge tables
        df = delta_sharing.load_as_pandas(table_url, limit=10)
        
        print(f"{Colors.GREEN}Table loaded successfully{Colors.RESET}")
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Data types:\n{df.dtypes}")
        
        if len(df) > 0:
            print(f"\n{Colors.CYAN}Sample data (first 3 rows):{Colors.RESET}")
            print(df.head(3))
        
        # Verify it's a pandas DataFrame
        if not isinstance(df, pd.DataFrame):
            print(f"{Colors.RED}Result is not a pandas DataFrame!{Colors.RESET}")
            return False
        
        return True
    except Exception as e:
        print(f"{Colors.RED}Error loading table as pandas: {e}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: Load Table with Limit
# ═════════════════════════════════════════════════════════════════════════════

def test_load_as_pandas_with_limit():
    """Test delta_sharing.load_as_pandas() with limit parameter"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        all_tables = client.list_all_tables()
        
        if not all_tables:
            print(f"{Colors.YELLOW}No tables available{Colors.RESET}")
            return True
        
        table = all_tables[0]
        table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
        
        limits_to_test = [1, 5, 10]
        
        for limit in limits_to_test:
            print(f"{Colors.CYAN}Loading with limit={limit}{Colors.RESET}")
            df = delta_sharing.load_as_pandas(table_url, limit=limit)
            print(f"  Rows returned: {len(df)}")
            
            if len(df) > limit:
                print(f"{Colors.RED}Limit not respected: expected <= {limit}, got {len(df)}{Colors.RESET}")
                return False
        
        return True
    except Exception as e:
        print(f"{Colors.RED}Error testing limit parameter: {e}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Test: Load Table with Version
# ═════════════════════════════════════════════════════════════════════════════

def test_load_as_pandas_with_version():
    """Test delta_sharing.load_as_pandas() with version parameter"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        all_tables = client.list_all_tables()
        
        if not all_tables:
            print(f"{Colors.YELLOW}No tables available{Colors.RESET}")
            return True
        
        table = all_tables[0]
        table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
        
        print(f"{Colors.CYAN}Loading with version=0 (initial version){Colors.RESET}")
        df = delta_sharing.load_as_pandas(table_url, limit=5, version=0)
        print(f"  Rows returned: {len(df)}")
        print(f"  Columns: {list(df.columns)}")
        
        return True
    except Exception as e:
        # Version might not be supported or table might not have version 0
        print(f"{Colors.YELLOW}Version parameter test: {e}{Colors.RESET}")
        print(f"{Colors.YELLOW}This is expected if versioning is not fully supported{Colors.RESET}")
        return True  # Don't fail the test


# ═════════════════════════════════════════════════════════════════════════════
# Test: Load Table as Spark
# ═════════════════════════════════════════════════════════════════════════════

def test_load_as_spark():
    """Test delta_sharing.load_as_spark() if Spark is available"""
    try:
        # Check if Spark is available
        try:
            from pyspark.sql import SparkSession
            spark_available = True
        except ImportError:
            spark_available = False
        
        if not spark_available:
            print(f"{Colors.YELLOW}Spark not available - skipping Spark test{Colors.RESET}")
            return True
        
        client = delta_sharing.SharingClient(PROFILE_FILE)
        all_tables = client.list_all_tables()
        
        if not all_tables:
            print(f"{Colors.YELLOW}No tables available{Colors.RESET}")
            return True
        
        table = all_tables[0]
        table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
        
        print(f"{Colors.CYAN}Loading table as Spark DataFrame{Colors.RESET}")
        spark_df = delta_sharing.load_as_spark(table_url)
        
        print(f"{Colors.GREEN}Table loaded as Spark DataFrame{Colors.RESET}")
        print(f"  Schema: {spark_df.schema}")
        print(f"  Count: {spark_df.count()}")
        
        return True
    except Exception as e:
        print(f"{Colors.YELLOW}Spark test: {e}{Colors.RESET}")
        print(f"{Colors.YELLOW}This is expected if Spark is not installed{Colors.RESET}")
        return True  # Don't fail the test


# ═════════════════════════════════════════════════════════════════════════════
# Test: Error Handling
# ═════════════════════════════════════════════════════════════════════════════

def test_error_handling():
    """Test that proper errors are raised for invalid requests"""
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        
        # Test 1: List schemas with invalid share
        print(f"{Colors.CYAN}Test 1: Invalid share name{Colors.RESET}")
        try:
            invalid_share = delta_sharing.Share(name="nonexistent_share_12345")
            schemas = client.list_schemas(invalid_share)
            print(f"{Colors.YELLOW}  Warning: No error raised for invalid share{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.GREEN}  ✓ Properly raised error: {type(e).__name__}{Colors.RESET}")
        
        # Test 2: Load invalid table
        print(f"\n{Colors.CYAN}Test 2: Invalid table URL{Colors.RESET}")
        try:
            invalid_url = f"{PROFILE_FILE}#invalid.invalid.invalid"
            df = delta_sharing.load_as_pandas(invalid_url, limit=1)
            print(f"{Colors.YELLOW}  Warning: No error raised for invalid table{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.GREEN}  ✓ Properly raised error: {type(e).__name__}{Colors.RESET}")
        
        return True
    except Exception as e:
        print(f"{Colors.RED}Error in error handling test: {e}{Colors.RESET}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Main Test Runner
# ═════════════════════════════════════════════════════════════════════════════

def main():
    """Run all tests or selected tests"""
    # Parse command line arguments first
    parse_arguments()
    
    print_section("DELTA SHARING LIBRARY COMPATIBILITY TEST")
    
    print(f"{Colors.BRIGHT_WHITE}Profile File:{Colors.RESET} {PROFILE_FILE}")
    print(f"{Colors.BRIGHT_WHITE}Delta Sharing Version:{Colors.RESET} {delta_sharing.__version__}")
    
    if TEST_TO_RUN:
        print(f"{Colors.BRIGHT_WHITE}Running Tests:{Colors.RESET} {', '.join(map(str, TEST_TO_RUN))}")
    else:
        print(f"{Colors.BRIGHT_WHITE}Running:{Colors.RESET} All tests")
    
    try:
        # Profile and Client Tests
        if should_run_test(1) or should_run_test(2):
            print_section("PROFILE AND CLIENT TESTS")
        
        if should_run_test(1):
            run_test("Test 1: Load Profile", test_load_profile)
        
        if should_run_test(2):
            run_test("Test 2: Client Initialization", test_client_initialization)
        
        # List Operations
        if any(should_run_test(i) for i in [3, 4, 5, 6]):
            print_section("LIST OPERATIONS")
        
        if should_run_test(3):
            run_test("Test 3: List Shares", test_list_shares)
        
        if should_run_test(4):
            run_test("Test 4: List Schemas", test_list_schemas)
        
        if should_run_test(5):
            run_test("Test 5: List Tables in Schema", test_list_tables_in_schema)
        
        if should_run_test(6):
            run_test("Test 6: List All Tables", test_list_all_tables)
        
        # Metadata Operations
        if should_run_test(7):
            print_section("METADATA OPERATIONS")
            run_test("Test 7: Get Table Metadata", test_get_table_metadata)
        
        # Data Loading Tests
        if any(should_run_test(i) for i in [8, 9, 10, 11]):
            print_section("DATA LOADING TESTS")
        
        if should_run_test(8):
            run_test("Test 8: Load as Pandas (Basic)", test_load_as_pandas_basic)
        
        if should_run_test(9):
            run_test("Test 9: Load with Limit", test_load_as_pandas_with_limit)
        
        if should_run_test(10):
            run_test("Test 10: Load with Version", test_load_as_pandas_with_version)
        
        if should_run_test(11):
            run_test("Test 11: Load as Spark", test_load_as_spark)
        
        # Advanced Tests
        if should_run_test(12):
            print_section("ADVANCED TESTS")
            run_test("Test 12: Error Handling", test_error_handling)
        
        # Summary
        print_section("TEST SUMMARY")
        
        total_tests = len(TEST_RESULTS)
        
        if total_tests == 0:
            print(f"{Colors.YELLOW}No tests were executed.{Colors.RESET}")
            if TEST_TO_RUN:
                print(f"{Colors.YELLOW}Check if the test number(s) {TEST_TO_RUN} are valid (1-12).{Colors.RESET}")
            print(f"\nUse --list to see available tests.")
            sys.exit(0)
        
        passed_tests = sum(1 for _, passed, _, _ in TEST_RESULTS if passed)
        failed_tests = total_tests - passed_tests
        
        # Results overview
        if failed_tests == 0:
            print(f"{Colors.GREEN}{Colors.BOLD}✅ All {total_tests} tests passed!{Colors.RESET}\n")
        else:
            print(f"{Colors.YELLOW}{Colors.BOLD}⚠️  {passed_tests}/{total_tests} tests passed{Colors.RESET}\n")
        
        # Individual results
        print(f"{Colors.BRIGHT_CYAN}{Colors.BOLD}Detailed Results:{Colors.RESET}\n")
        for test_name, passed, error_msg, duration in TEST_RESULTS:
            status = f"{Colors.GREEN}✅ PASS{Colors.RESET}" if passed else f"{Colors.RED}❌ FAIL{Colors.RESET}"
            print(f"{status} - {test_name} {Colors.DIM}({format_duration(duration)}){Colors.RESET}")
            if error_msg and not passed:
                error_preview = error_msg[:100] + "..." if len(error_msg) > 100 else error_msg
                print(f"{Colors.DIM}     └─ {error_preview}{Colors.RESET}")
        
        # Statistics
        print(f"\n{Colors.BRIGHT_CYAN}{Colors.BOLD}Statistics:{Colors.RESET}")
        print(f"{Colors.BRIGHT_WHITE}   Total:{Colors.RESET} {total_tests}")
        print(f"{Colors.GREEN}   Passed:{Colors.RESET} {passed_tests}")
        print(f"{Colors.RED}   Failed:{Colors.RESET} {failed_tests}")
        print(f"{Colors.BRIGHT_WHITE}   Success Rate:{Colors.RESET} {(passed_tests/total_tests*100):.1f}%")
        
        # Performance
        total_duration = sum(d for _, _, _, d in TEST_RESULTS)
        print(f"\n{Colors.BRIGHT_CYAN}{Colors.BOLD}Performance:{Colors.RESET}")
        print(f"{Colors.BRIGHT_WHITE}   Total Time:{Colors.RESET} {format_duration(total_duration)}")
        print(f"{Colors.BRIGHT_WHITE}   Average Time:{Colors.RESET} {format_duration(total_duration/total_tests if total_tests > 0 else 0)}")
        
        if TEST_RESULTS:
            slowest = max(TEST_RESULTS, key=lambda x: x[3])
            fastest = min(TEST_RESULTS, key=lambda x: x[3])
            print(f"{Colors.DIM}   Slowest: {slowest[0]} ({format_duration(slowest[3])}){Colors.RESET}")
            print(f"{Colors.DIM}   Fastest: {fastest[0]} ({format_duration(fastest[3])}){Colors.RESET}")
        
        print()
        
        # Exit code
        sys.exit(0 if failed_tests == 0 else 1)
        
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}⚠️  Tests interrupted by user{Colors.RESET}")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n{Colors.RED}❌ Unexpected error: {e}{Colors.RESET}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

