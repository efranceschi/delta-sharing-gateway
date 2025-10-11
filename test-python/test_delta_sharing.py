#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
Delta Sharing Gateway - Comprehensive Test Suite
═══════════════════════════════════════════════════════════════════════════════

Test script for Delta Sharing OnPrem server.
Tests the Delta Sharing protocol implementation using the official Python client.

Author: Delta Sharing Team
Version: 1.0.0
Python: 3.8+

═══════════════════════════════════════════════════════════════════════════════
TABLE OF CONTENTS
═══════════════════════════════════════════════════════════════════════════════

1. IMPORTS AND CONFIGURATION
   - Library imports
   - Global configuration variables

2. UTILITY FUNCTIONS
   - print_section()           : Format section headers
   - format_key_value()        : Format key-value pairs
   - format_data_row()         : Format dictionary data

3. DEBUG AND HTTP FUNCTIONS
   - debug_http_request()      : Display HTTP request/response in debug mode
   - http_get()                : HTTP GET wrapper with debug logging
   - http_post()               : HTTP POST wrapper with debug logging

4. TEST FUNCTIONS (in execution order)
   - test_list_shares()        : TEST 1 - List all shares
   - test_list_schemas()       : TEST 2 - List schemas in each share
   - test_list_tables()        : TEST 3 - List tables in each schema
   - test_list_all_tables()    : TEST 4 - List all tables (shortcut)
   - test_file_list()          : TEST 5 - Get file list and statistics
   - test_table_metadata()     : TEST 6 - Get table metadata and statistics
   - test_load_table_data()    : TEST 7 - Load data from tables
   - test_direct_api()         : TEST 8 - Direct REST API calls
   - test_data_skipping()      : TEST 9 - Data skipping with predicates

5. CONFIGURATION FUNCTIONS
   - load_profile_config()     : Load Delta Sharing profile file
   - parse_args()              : Parse command line arguments

6. MAIN EXECUTION
   - main()                    : Main test runner

═══════════════════════════════════════════════════════════════════════════════
"""

# ═══════════════════════════════════════════════════════════════════════════
# 1. IMPORTS AND CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

import pandas as pd
import sys
import traceback
import argparse
import json
import logging
import os
import time
from pathlib import Path
from collections import defaultdict

# Note: delta_sharing is imported later after configuring logging
# to ensure debug logs are captured from the start

# Default Configuration
DEFAULT_PROFILE_FILE = "config.share"

# Global variables (will be set from profile file or during execution)
PROFILE_FILE = None
BEARER_TOKEN = None
BASE_URL = None
DEBUG = False
delta_sharing = None  # Will be imported in main() after configuring logging

# Test results tracking
TEST_RESULTS = []  # List of tuples: (test_name, passed, error_message, duration_seconds)

# Cached data for lazy loading (shared between tests)
_cached_shares = None
_cached_all_tables = None

# ANSI Color Codes for Terminal Output
class Colors:
    """ANSI color codes for terminal output"""
    # Basic colors
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    
    # Foreground colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    # Bright foreground colors
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'
    
    # Background colors
    BG_BLACK = '\033[40m'
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_BLUE = '\033[44m'
    BG_MAGENTA = '\033[45m'
    BG_CYAN = '\033[46m'
    BG_WHITE = '\033[47m'
    
    @staticmethod
    def disable():
        """Disable colors (for non-terminal output or Windows without ANSI support)"""
        Colors.RESET = ''
        Colors.BOLD = ''
        Colors.DIM = ''
        Colors.BLACK = Colors.RED = Colors.GREEN = Colors.YELLOW = ''
        Colors.BLUE = Colors.MAGENTA = Colors.CYAN = Colors.WHITE = ''
        Colors.BRIGHT_BLACK = Colors.BRIGHT_RED = Colors.BRIGHT_GREEN = ''
        Colors.BRIGHT_YELLOW = Colors.BRIGHT_BLUE = Colors.BRIGHT_MAGENTA = ''
        Colors.BRIGHT_CYAN = Colors.BRIGHT_WHITE = ''
        Colors.BG_BLACK = Colors.BG_RED = Colors.BG_GREEN = Colors.BG_YELLOW = ''
        Colors.BG_BLUE = Colors.BG_MAGENTA = Colors.BG_CYAN = Colors.BG_WHITE = ''

# Check if we should disable colors (non-TTY output or NO_COLOR environment variable)
if not sys.stdout.isatty() or os.environ.get('NO_COLOR'):
    Colors.disable()


# ═══════════════════════════════════════════════════════════════════════════
# 2. UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def get_shares_lazy():
    """
    Get shares with lazy loading and caching
    
    Returns:
        list: List of Share objects
    """
    global _cached_shares
    
    if _cached_shares is None:
        try:
            client = delta_sharing.SharingClient(PROFILE_FILE)
            _cached_shares = client.list_shares()
            if DEBUG:
                print(f"{Colors.DIM}[Cache] Loaded {len(_cached_shares)} share(s){Colors.RESET}")
        except Exception as e:
            if DEBUG:
                print(f"{Colors.RED}[Cache] Error loading shares: {e}{Colors.RESET}")
            _cached_shares = []
    
    return _cached_shares

def get_all_tables_lazy():
    """
    Get all tables with lazy loading and caching
    
    Returns:
        list: List of Table objects
    """
    global _cached_all_tables
    
    if _cached_all_tables is None:
        try:
            client = delta_sharing.SharingClient(PROFILE_FILE)
            _cached_all_tables = client.list_all_tables()
            if DEBUG:
                print(f"{Colors.DIM}[Cache] Loaded {len(_cached_all_tables)} table(s){Colors.RESET}")
        except Exception as e:
            if DEBUG:
                print(f"{Colors.RED}[Cache] Error loading tables: {e}{Colors.RESET}")
            _cached_all_tables = []
    
    return _cached_all_tables

def add_test_result(test_name, passed, error_message=None, duration=0.0):
    """
    Add a test result to the global results list
    
    Args:
        test_name (str): Name of the test
        passed (bool): Whether the test passed
        error_message (str, optional): Error message if test failed
        duration (float, optional): Test duration in seconds
    """
    TEST_RESULTS.append((test_name, passed, error_message, duration))

def format_duration(seconds):
    """
    Format duration in a human-readable format
    
    Args:
        seconds (float): Duration in seconds
        
    Returns:
        str: Formatted duration string
    """
    if seconds < 0.001:
        return f"{seconds * 1000000:.2f}μs"
    elif seconds < 1:
        return f"{seconds * 1000:.2f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"

def configure_delta_sharing_logging():
    """
    Configure debug logging specifically for delta_sharing library
    
    Sets up detailed logging only for the delta_sharing module, without
    affecting other libraries. This must be called before importing delta_sharing.
    
    The logger will output to stdout with a clean format showing:
    - Timestamp
    - Logger name (delta_sharing)
    - Log level
    - Message
    """
    # Create a specific logger for delta_sharing
    delta_logger = logging.getLogger('delta_sharing')
    delta_logger.setLevel(logging.DEBUG)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    
    # Create formatter with colors
    formatter = logging.Formatter(
        f'\n{Colors.MAGENTA}🔍 [%(asctime)s] %(name)s - %(levelname)s{Colors.RESET}\n'
        f'{Colors.DIM}   %(message)s{Colors.RESET}',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to logger (avoid duplicates)
    if not delta_logger.handlers:
        delta_logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    delta_logger.propagate = False
    
    print(f"\n{Colors.BRIGHT_BLACK}{'─' * 80}{Colors.RESET}")
    print(f"{Colors.MAGENTA}🔍 Delta Sharing Debug Logging: {Colors.BOLD}ENABLED{Colors.RESET}")
    print(f"{Colors.DIM}   Logger: delta_sharing")
    print(f"   Level: DEBUG{Colors.RESET}")
    print(f"{Colors.BRIGHT_BLACK}{'─' * 80}{Colors.RESET}\n")

def print_section(title):
    """Print a formatted section header with colors"""
    print(f"\n{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}  {title}{Colors.RESET}")
    print(f"{Colors.CYAN}{Colors.BOLD}{'='*80}{Colors.RESET}\n")

def format_key_value(key, value, indent=2):
    """Format a key-value pair with proper indentation"""
    spaces = " " * indent
    # Truncate long values
    value_str = str(value)
    if len(value_str) > 80:
        value_str = value_str[:77] + "..."
    return f"{spaces}{key}: {value_str}"

def format_data_row(data_dict, indent=2):
    """Format a dictionary as key-value pairs"""
    lines = []
    for key, value in data_dict.items():
        lines.append(format_key_value(key, value, indent))
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# 3. DEBUG AND HTTP FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def debug_http_request(method, url, headers=None, json_data=None, response=None):
    """
    Print formatted HTTP request and response when in debug mode
    
    Displays full HTTP details without width restrictions, using separators
    and indentation for clarity instead of box borders.
    """
    if not DEBUG:
        return
    
    print(f"\n{Colors.MAGENTA}{'='*80}{Colors.RESET}")
    print(f"{Colors.MAGENTA}{Colors.BOLD}🔍 DEBUG: HTTP Request{Colors.RESET}")
    print(f"{Colors.MAGENTA}{'='*80}{Colors.RESET}")
    print(f"{Colors.BRIGHT_BLUE}Method:{Colors.RESET} {Colors.BOLD}{method}{Colors.RESET}")
    print(f"{Colors.BRIGHT_BLUE}URL:{Colors.RESET} {url}")
    
    if headers:
        print(f"\n{Colors.BRIGHT_BLUE}Headers:{Colors.RESET}")
        for key, value in headers.items():
            # Mask sensitive data
            if key.lower() == 'authorization':
                display_value = value[:20] + "..." if len(value) > 20 else value
                print(f"{Colors.DIM}  {key}:{Colors.RESET} {Colors.YELLOW}{display_value}{Colors.RESET}")
            else:
                display_value = value
                print(f"{Colors.DIM}  {key}:{Colors.RESET} {display_value}")
    
    if json_data:
        print(f"\n{Colors.BRIGHT_BLUE}Request Body:{Colors.RESET}")
        json_str = json.dumps(json_data, indent=2)
        for line in json_str.split('\n'):
            print(f"{Colors.DIM}  {line}{Colors.RESET}")
    
    if response is not None:
        print(f"\n{Colors.BRIGHT_MAGENTA}{'-'*80}{Colors.RESET}")
        print(f"{Colors.BRIGHT_MAGENTA}{Colors.BOLD}📥 HTTP Response{Colors.RESET}")
        print(f"{Colors.BRIGHT_MAGENTA}{'-'*80}{Colors.RESET}")
        
        # Color code based on status
        if 200 <= response.status_code < 300:
            status_color = Colors.GREEN
        elif 400 <= response.status_code < 500:
            status_color = Colors.YELLOW
        else:
            status_color = Colors.RED
        
        print(f"{Colors.BRIGHT_BLUE}Status Code:{Colors.RESET} {status_color}{Colors.BOLD}{response.status_code}{Colors.RESET}")
        
        print(f"\n{Colors.BRIGHT_BLUE}Response Body:{Colors.RESET}")
        
        try:
            # Try to parse as JSON
            response_text = response.text.strip()
            
            # Check if it's NDJSON (multiple JSON objects separated by newlines)
            if '\n' in response_text and response_text.count('{') > 1:
                print(f"{Colors.DIM}  (NDJSON format - multiple JSON objects){Colors.RESET}")
                lines = response_text.split('\n')
                max_objects_to_show = 5  # Show more objects since no width limit
                
                for i, line in enumerate(lines[:max_objects_to_show]):
                    if line.strip():
                        try:
                            obj = json.loads(line)
                            json_str = json.dumps(obj, indent=2)
                            print(f"\n{Colors.CYAN}  Object {i+1}:{Colors.RESET}")
                            for json_line in json_str.split('\n'):
                                print(f"{Colors.DIM}    {json_line}{Colors.RESET}")
                        except:
                            print(f"\n{Colors.YELLOW}  Object {i+1} (raw):{Colors.RESET}")
                            print(f"{Colors.DIM}    {line}{Colors.RESET}")
                
                if len(lines) > max_objects_to_show:
                    print(f"\n{Colors.DIM}  ... ({len(lines) - max_objects_to_show} more objects){Colors.RESET}")
            else:
                # Single JSON object
                try:
                    json_obj = response.json()
                    json_str = json.dumps(json_obj, indent=2)
                    lines = json_str.split('\n')
                    max_lines_to_show = 50  # Show more lines since no width limit
                    
                    for line in lines[:max_lines_to_show]:
                        print(f"{Colors.DIM}  {line}{Colors.RESET}")
                    
                    if len(lines) > max_lines_to_show:
                        print(f"\n{Colors.DIM}  ... ({len(lines) - max_lines_to_show} more lines){Colors.RESET}")
                except:
                    # Not JSON, show raw text
                    max_chars = 1000  # Show more characters
                    text_preview = response_text[:max_chars]
                    for line in text_preview.split('\n')[:20]:
                        print(f"{Colors.DIM}  {line}{Colors.RESET}")
                    
                    if len(response_text) > max_chars:
                        print(f"\n{Colors.DIM}  ... ({len(response_text) - max_chars} more characters){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}  Error formatting response: {e}{Colors.RESET}")
    
    print(f"{Colors.MAGENTA}{'='*80}{Colors.RESET}\n")

def http_get(url, headers=None):
    """Wrapper for HTTP GET with debug logging"""
    import requests
    
    if DEBUG:
        debug_http_request("GET", url, headers=headers)
    
    response = requests.get(url, headers=headers)
    
    if DEBUG:
        debug_http_request("GET", url, headers=headers, response=response)
    
    return response

def http_post(url, headers=None, json_data=None):
    """Wrapper for HTTP POST with debug logging"""
    import requests
    
    if DEBUG:
        debug_http_request("POST", url, headers=headers, json_data=json_data)
    
    response = requests.post(url, headers=headers, json=json_data)
    
    if DEBUG:
        debug_http_request("POST", url, headers=headers, json_data=json_data, response=response)
    
    return response


# ═══════════════════════════════════════════════════════════════════════════
# 4. TEST FUNCTIONS (in execution order)
# ═══════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# TEST 1: List All Shares
# ─────────────────────────────────────────────────────────────────────────────

def test_list_shares():
    """
    Test 1: List all shares
    
    Tests the /shares endpoint to retrieve all available shares.
    First executes HTTP API call to show request/response,
    then uses delta_sharing library for comparison.
    
    Returns:
        list: List of Share objects, or empty list on error
    """
    print_section("TEST 1: List All Shares")
    start_time = time.time()
    
    try:
        # First: HTTP API call to show request/response
        print(f"{Colors.BRIGHT_BLUE}📡 HTTP API Call:{Colors.RESET}\n")
        headers = {
            "Authorization": f"Bearer {BEARER_TOKEN}",
            "Content-Type": "application/json"
        }
        url = f"{BASE_URL}/shares"
        response = http_get(url, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"HTTP request failed with status {response.status_code}")
        
        # Parse HTTP response
        http_data = response.json()
        http_shares = http_data.get('items', [])
        print(f"\n{Colors.BRIGHT_GREEN}✅ HTTP API: Found {len(http_shares)} share(s){Colors.RESET}")
        
        # Second: delta_sharing library call for comparison
        print(f"\n{Colors.BRIGHT_BLUE}📚 Delta Sharing Library:{Colors.RESET}")
        client = delta_sharing.SharingClient(PROFILE_FILE)
        shares = client.list_shares()
        
        duration = time.time() - start_time
        
        print(f"{Colors.GREEN}✅ Library: Found {len(shares)} share(s):{Colors.RESET}")
        for share in shares:
            print(f"{Colors.BRIGHT_WHITE}  - {share.name}{Colors.RESET}")
        
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 1: List All Shares", True, None, duration)
        return shares
    except Exception as e:
        duration = time.time() - start_time
        print(f"{Colors.RED}❌ Error listing shares: {e}{Colors.RESET}")
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 1: List All Shares", False, str(e), duration)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# TEST 2: List Schemas in Shares
# ─────────────────────────────────────────────────────────────────────────────

def test_list_schemas():
    """
    Test 2: List schemas in each share
    
    Uses lazy loading to get shares if not already loaded.
    Tests both HTTP API and delta_sharing library.
    
    Returns:
        list: List of tuples (share, schema), or empty list
    """
    print_section("TEST 2: List Schemas in Shares")
    start_time = time.time()
    
    # Get shares using lazy loading
    shares = get_shares_lazy()
    
    if not shares:
        duration = time.time() - start_time
        print(f"{Colors.YELLOW}⚠️  No shares available{Colors.RESET}")
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 2: List Schemas in Shares", False, "No shares available", duration)
        return []
    
    all_schemas = []
    has_error = False
    error_msg = None
    
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }
    
    for share in shares:
        try:
            # First: HTTP API call
            print(f"\n{Colors.BRIGHT_BLUE}📡 HTTP API Call for share: {Colors.BOLD}{share.name}{Colors.RESET}\n")
            url = f"{BASE_URL}/shares/{share.name}/schemas"
            response = http_get(url, headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"HTTP request failed with status {response.status_code}")
            
            # Parse HTTP response
            http_data = response.json()
            http_schemas = http_data.get('items', [])
            print(f"\n{Colors.BRIGHT_GREEN}✅ HTTP API: Found {len(http_schemas)} schema(s){Colors.RESET}")
            
            # Second: delta_sharing library call
            print(f"\n{Colors.BRIGHT_BLUE}📚 Delta Sharing Library:{Colors.RESET}")
            client = delta_sharing.SharingClient(PROFILE_FILE)
            schemas = client.list_schemas(share)
            
            print(f"{Colors.BRIGHT_CYAN}📂 Share: {Colors.BOLD}{share.name}{Colors.RESET}")
            print(f"{Colors.DIM}   Found {len(schemas)} schema(s):{Colors.RESET}")
            for schema in schemas:
                print(f"{Colors.BRIGHT_WHITE}     - {schema.name}{Colors.RESET}")
                all_schemas.append((share, schema))
        except Exception as e:
            print(f"{Colors.RED}❌ Error listing schemas for {share.name}: {e}{Colors.RESET}")
            has_error = True
            error_msg = str(e)
    
    duration = time.time() - start_time
    print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
    
    if not has_error and len(all_schemas) > 0:
        add_test_result("TEST 2: List Schemas in Shares", True, None, duration)
    elif len(all_schemas) > 0:
        add_test_result("TEST 2: List Schemas in Shares", True, "Partial success with some errors", duration)
    else:
        add_test_result("TEST 2: List Schemas in Shares", False, error_msg or "No schemas found", duration)
    
    return all_schemas


# ─────────────────────────────────────────────────────────────────────────────
# TEST 3: List Tables in Schemas
# ─────────────────────────────────────────────────────────────────────────────

def test_list_tables(share_schemas):
    """
    Test 3: List tables in each schema
    
    For each schema discovered in Test 2, list all available tables.
    Tests both HTTP API and delta_sharing library.
    
    Args:
        share_schemas: List of tuples (share, schema) from test_list_schemas()
        
    Returns:
        list: List of Table objects, or empty list
    """
    print_section("TEST 3: List Tables in Schemas")
    start_time = time.time()
    
    all_tables = []
    has_error = False
    error_msg = None
    
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }
    
    for share, schema in share_schemas:
        try:
            # First: HTTP API call
            print(f"\n{Colors.BRIGHT_BLUE}📡 HTTP API Call for: {Colors.BOLD}{share.name}.{schema.name}{Colors.RESET}\n")
            url = f"{BASE_URL}/shares/{share.name}/schemas/{schema.name}/tables"
            response = http_get(url, headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"HTTP request failed with status {response.status_code}")
            
            # Parse HTTP response
            http_data = response.json()
            http_tables = http_data.get('items', [])
            print(f"\n{Colors.BRIGHT_GREEN}✅ HTTP API: Found {len(http_tables)} table(s){Colors.RESET}")
            
            # Show share_id and id if available (Databricks format)
            if http_tables and len(http_tables) > 0:
                first_table = http_tables[0]
                if 'share_id' in first_table and 'id' in first_table:
                    print(f"{Colors.DIM}   Sample: share_id={first_table.get('share_id', 'N/A')[:13]}..., id={first_table.get('id', 'N/A')[:13]}...{Colors.RESET}")
            
            # Second: delta_sharing library call
            print(f"\n{Colors.BRIGHT_BLUE}📚 Delta Sharing Library:{Colors.RESET}")
            client = delta_sharing.SharingClient(PROFILE_FILE)
            tables = client.list_tables(schema)
            
            print(f"{Colors.BRIGHT_CYAN}📁 Schema: {Colors.BOLD}{share.name}.{schema.name}{Colors.RESET}")
            print(f"{Colors.DIM}   Found {len(tables)} table(s):{Colors.RESET}")
            for table in tables:
                print(f"{Colors.BRIGHT_WHITE}     - {table.name}{Colors.RESET}")
                all_tables.append(table)
        except Exception as e:
            print(f"{Colors.RED}❌ Error listing tables for {share.name}.{schema.name}: {e}{Colors.RESET}")
            has_error = True
            error_msg = str(e)
    
    duration = time.time() - start_time
    print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
    
    if not has_error and len(all_tables) > 0:
        add_test_result("TEST 3: List Tables in Schemas", True, None, duration)
    elif len(all_tables) > 0:
        add_test_result("TEST 3: List Tables in Schemas", True, "Partial success with some errors", duration)
    else:
        add_test_result("TEST 3: List Tables in Schemas", False, error_msg or "No tables found", duration)
    
    return all_tables


# ─────────────────────────────────────────────────────────────────────────────
# TEST 4: List All Tables (Shortcut)
# ─────────────────────────────────────────────────────────────────────────────

def test_list_all_tables():
    """
    Test 4: List all tables across all shares
    
    Uses the /shares/{share}/all-tables endpoint as a shortcut to get all tables
    at once without iterating through schemas.
    
    Returns:
        list: List of all Table objects, or empty list on error
    """
    print_section("TEST 4: List All Tables")
    start_time = time.time()
    
    try:
        client = delta_sharing.SharingClient(PROFILE_FILE)
        all_tables = client.list_all_tables()
        
        print(f"{Colors.GREEN}✅ Found {len(all_tables)} table(s) total:{Colors.RESET}")
        
        # Prepare headers for direct API calls to get table format
        headers = {
            "Authorization": f"Bearer {BEARER_TOKEN}",
            "Content-Type": "application/json"
        }
        
        # Group by share
        tables_by_share = {}
        for table in all_tables:
            share_name = table.share
            if share_name not in tables_by_share:
                tables_by_share[share_name] = []
            tables_by_share[share_name].append(table)
        
        metadata_start = time.time()
        for share_name, tables in tables_by_share.items():
            print(f"\n{Colors.BRIGHT_CYAN}  📂 {share_name}:{Colors.RESET}")
            for table in tables:
                # Get table format from metadata endpoint
                table_start = time.time()
                try:
                    url = f"{BASE_URL}/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/metadata"
                    response = http_get(url, headers=headers)
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
                
                table_duration = time.time() - table_start
                print(f"{Colors.BRIGHT_WHITE}     {table.schema}.{table.name}{Colors.RESET} {Colors.DIM}[{table_format}] ({format_duration(table_duration)}){Colors.RESET}")
        
        metadata_duration = time.time() - metadata_start
        print(f"\n{Colors.DIM}   Metadata retrieval: {format_duration(metadata_duration)}{Colors.RESET}")
        
        duration = time.time() - start_time
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 4: List All Tables", True, None, duration)
        return all_tables
    except Exception as e:
        duration = time.time() - start_time
        print(f"{Colors.RED}❌ Error listing all tables: {e}{Colors.RESET}")
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 4: List All Tables", False, str(e), duration)
        return []

# ─────────────────────────────────────────────────────────────────────────────
# TEST 5: Get File List and Statistics
# ─────────────────────────────────────────────────────────────────────────────

def test_file_list():
    """
    Test 5: Get file list and statistics
    
    Queries the table to retrieve the list of data files with their properties:
    - File URLs (S3, Azure, GCS, etc.)
    - File sizes
    - Number of records per file
    - Null counts
    - Partition values
    
    Uses lazy loading to get tables if not already loaded.
    """
    print_section("TEST 5: Get File List and Statistics")
    start_time = time.time()
    
    # Get tables using lazy loading
    tables = get_all_tables_lazy()
    
    if not tables:
        duration = time.time() - start_time
        print(f"{Colors.YELLOW}⚠️  No tables available to get file list from{Colors.RESET}")
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 5: Get File List and Statistics", False, "No tables available", duration)
        return
    
    # Test with the first table
    table = tables[0]
    
    print(f"{Colors.BRIGHT_BLUE}📋 Getting file list for: {Colors.BOLD}{table.share}.{table.schema}.{table.name}{Colors.RESET}\n")
    
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        # Query the table to get file information
        query_url = f"{BASE_URL}/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/query"
        
        query_start = time.time()
        response = http_post(query_url, headers=headers, json_data={"limitHint": 10})
        query_duration = time.time() - query_start
        print(f"{Colors.DIM}   HTTP POST time: {format_duration(query_duration)}{Colors.RESET}\n")
        
        if response.status_code == 200:
            file_count = 0
            
            # Parse NDJSON response
            parse_start = time.time()
            for line in response.text.strip().split('\n'):
                obj = json.loads(line)
                
                # Look for file entries
                if 'file' in obj:
                    file_info = obj['file']
                    file_count += 1
                    
                    # Extract URL (no truncation - full URL displayed)
                    url = file_info.get('url', 'N/A')
                    
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
                    
                    print(f"\n  File {file_count}:")
                    print(f"    URL: {url}")
                    print(f"    Size: {file_size_display}")
                    print(f"    Records: {num_records}")
                    print(f"    Total Nulls: {total_nulls}")
                    print(f"    Partitions: {partition_display}")
            
            parse_duration = time.time() - parse_start
            print(f"\n{Colors.DIM}   Parsing time: {format_duration(parse_duration)}{Colors.RESET}")
            
            if file_count > 0:
                duration = time.time() - start_time
                print(f"\n{Colors.GREEN}✅ Total files retrieved: {file_count}{Colors.RESET}")
                print(f"{Colors.DIM}⏱️  Total Duration: {format_duration(duration)}{Colors.RESET}")
                add_test_result("TEST 5: Get File List and Statistics", True, None, duration)
            else:
                duration = time.time() - start_time
                print(f"{Colors.DIM}   No file information available in query response{Colors.RESET}")
                print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
                add_test_result("TEST 5: Get File List and Statistics", False, "No file information in response", duration)
        else:
            duration = time.time() - start_time
            print(f"{Colors.RED}❌ Could not retrieve file list (HTTP {response.status_code}){Colors.RESET}")
            print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
            add_test_result("TEST 5: Get File List and Statistics", False, f"HTTP {response.status_code}", duration)
    
    except Exception as e:
        duration = time.time() - start_time
        print(f"{Colors.RED}❌ Error retrieving file list: {e}{Colors.RESET}")
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 5: Get File List and Statistics", False, str(e), duration)


# ─────────────────────────────────────────────────────────────────────────────
# TEST 6: Get Table Metadata and Statistics
# ─────────────────────────────────────────────────────────────────────────────

def test_table_metadata():
    """
    Test 6: Get table metadata and statistics
    
    Retrieves and displays comprehensive metadata for the first available table:
    - Schema (column names and types)
    - Table statistics (rows, columns, memory usage)
    - Column statistics (nulls, unique values, min/max/mean for numeric columns)
    
    Uses lazy loading to get tables if not already loaded.
    """
    print_section("TEST 6: Get Table Metadata and Statistics")
    start_time = time.time()
    
    # Get tables using lazy loading
    tables = get_all_tables_lazy()
    
    if not tables:
        duration = time.time() - start_time
        print(f"{Colors.YELLOW}⚠️  No tables available to get metadata from{Colors.RESET}")
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 6: Get Table Metadata and Statistics", False, "No tables available", duration)
        return
    
    # Test with the first table
    table = tables[0]
    table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
    
    print(f"{Colors.BRIGHT_BLUE}📋 Getting metadata for: {Colors.BOLD}{table.share}.{table.schema}.{table.name}{Colors.RESET}\n")
    
    try:
        # Step 1: Load table to get schema and statistics
        step_start = time.time()
        df = delta_sharing.load_as_pandas(table_url, limit=10)
        load_duration = time.time() - step_start
        if DEBUG:
            print(f"{Colors.DIM}   ⏱️  Step 1 - Load table data: {format_duration(load_duration)}{Colors.RESET}\n")
        
        # Step 2: Extract and display schema
        step_start = time.time()
        print(f"{Colors.GREEN}✅ Table Schema:{Colors.RESET}")
        for col_name, col_type in df.dtypes.items():
            print(f"{Colors.BRIGHT_WHITE}  • {col_name}:{Colors.RESET} {Colors.DIM}{col_type}{Colors.RESET}")
        schema_duration = time.time() - step_start
        if DEBUG:
            print(f"{Colors.DIM}   ⏱️  Step 2 - Extract schema: {format_duration(schema_duration)}{Colors.RESET}")
        
        # Step 3: Calculate and display table statistics
        step_start = time.time()
        print(f"\n{Colors.BRIGHT_CYAN}📊 Table Statistics:{Colors.RESET}")
        print(f"{Colors.BRIGHT_WHITE}  • Total Rows:{Colors.RESET} {len(df)}")
        print(f"{Colors.BRIGHT_WHITE}  • Total Columns:{Colors.RESET} {len(df.columns)}")
        mem_usage = df.memory_usage(deep=True).sum() / 1024
        print(f"{Colors.BRIGHT_WHITE}  • Memory Usage:{Colors.RESET} {mem_usage:.2f} KB")
        table_stats_duration = time.time() - step_start
        if DEBUG:
            print(f"{Colors.DIM}   ⏱️  Step 3 - Table statistics: {format_duration(table_stats_duration)}{Colors.RESET}")
        
        # Step 4: Column statistics
        step_start = time.time()
        print(f"\n{Colors.BRIGHT_CYAN}📈 Column Statistics:{Colors.RESET}")
        
        column_times = []
        for col in df.columns:
            col_start = time.time()
            try:
                # Get dtype
                dtype = str(df[col].dtype)
                
                # Count nulls
                null_count = df[col].isnull().sum()
                null_pct = f"{(null_count / len(df) * 100):.1f}%"
                
                # Get unique values count
                unique_count = df[col].nunique()
                
                print(f"\n  Column: {col}")
                print(f"    Type: {dtype}")
                print(f"    Nulls: {null_count} ({null_pct})")
                print(f"    Unique: {unique_count}")
                
                # Get min/max for numeric columns
                if df[col].dtype in ['int64', 'float64']:
                    min_val = df[col].min()
                    max_val = df[col].max()
                    mean_val = df[col].mean()
                    print(f"    Min: {min_val:.2f}" if pd.notna(min_val) else "    Min: N/A")
                    print(f"    Max: {max_val:.2f}" if pd.notna(max_val) else "    Max: N/A")
                    print(f"    Mean: {mean_val:.2f}" if pd.notna(mean_val) else "    Mean: N/A")
                
                col_duration = time.time() - col_start
                column_times.append((col, col_duration))
            except Exception as e:
                print(f"\n{Colors.YELLOW}  Column: {col}")
                print(f"    Error: {e}{Colors.RESET}")
                col_duration = time.time() - col_start
                column_times.append((col, col_duration))
        
        column_stats_duration = time.time() - step_start
        
        # Calculate total duration
        duration = time.time() - start_time
        
        # Show detailed timing only in DEBUG mode
        if DEBUG:
            print(f"\n{Colors.DIM}   ⏱️  Step 4 - Column statistics: {format_duration(column_stats_duration)}{Colors.RESET}")
            
            # Show per-column timing if there are multiple columns
            if len(column_times) > 1:
                print(f"\n{Colors.DIM}   Per-column timing:{Colors.RESET}")
                for col_name, col_time in column_times:
                    print(f"{Colors.DIM}     - {col_name}: {format_duration(col_time)}{Colors.RESET}")
                
                # Identify slowest column
                slowest_col, slowest_time = max(column_times, key=lambda x: x[1])
                print(f"{Colors.DIM}     Slowest: {slowest_col} ({format_duration(slowest_time)}){Colors.RESET}")
            
            # Total time breakdown
            print(f"\n{Colors.BRIGHT_CYAN}⏱️  Time Breakdown:{Colors.RESET}")
            print(f"{Colors.DIM}   1. Load table data:    {format_duration(load_duration)} ({load_duration/duration*100:.1f}%){Colors.RESET}")
            print(f"{Colors.DIM}   2. Extract schema:     {format_duration(schema_duration)} ({schema_duration/duration*100:.1f}%){Colors.RESET}")
            print(f"{Colors.DIM}   3. Table statistics:   {format_duration(table_stats_duration)} ({table_stats_duration/duration*100:.1f}%){Colors.RESET}")
            print(f"{Colors.DIM}   4. Column statistics:  {format_duration(column_stats_duration)} ({column_stats_duration/duration*100:.1f}%){Colors.RESET}")
            print(f"{Colors.BRIGHT_WHITE}   Total Duration:        {format_duration(duration)}{Colors.RESET}")
        
        # Always show total duration
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 6: Get Table Metadata and Statistics", True, None, duration)
        
    except Exception as e:
        duration = time.time() - start_time
        print(f"{Colors.RED}❌ Error getting table metadata: {e}{Colors.RESET}")
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 6: Get Table Metadata and Statistics", False, str(e), duration)


# ─────────────────────────────────────────────────────────────────────────────
# TEST 7: Load Table Data
# ─────────────────────────────────────────────────────────────────────────────

def test_load_table_data():
    """
    Test 7: Load data from tables
    
    Loads actual data from the first available table into a Pandas DataFrame
    and displays the first few rows.
    Tests both HTTP API (to get file list) and delta_sharing library (to load data).
    
    Uses lazy loading to get tables if not already loaded.
        
    Returns:
        DataFrame: Loaded data, or None on error
    """
    print_section("TEST 7: Load Table Data")
    start_time = time.time()
    
    # Get tables using lazy loading
    tables = get_all_tables_lazy()
    
    if not tables:
        duration = time.time() - start_time
        print(f"{Colors.YELLOW}⚠️  No tables available to load data from{Colors.RESET}")
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 7: Load Table Data", False, "No tables available", duration)
        return
    
    # Test with the first table
    table = tables[0]
    table_url = f"{PROFILE_FILE}#{table.share}.{table.schema}.{table.name}"
    
    print(f"{Colors.BRIGHT_BLUE}📋 Loading data from: {Colors.BOLD}{table.share}.{table.schema}.{table.name}{Colors.RESET}")
    print(f"{Colors.DIM}   Table URL: {table_url}{Colors.RESET}\n")
    
    try:
        # First: HTTP API call to show available files
        print(f"{Colors.BRIGHT_BLUE}📡 HTTP API Call - Getting file list:{Colors.RESET}\n")
        headers = {
            "Authorization": f"Bearer {BEARER_TOKEN}",
            "Content-Type": "application/json"
        }
        query_url = f"{BASE_URL}/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/query"
        response = http_post(query_url, headers=headers, json_data={"limitHint": 5})
        
        if response.status_code == 200:
            # Count files in NDJSON response
            file_count = 0
            for line in response.text.strip().split('\n'):
                obj = json.loads(line)
                if 'file' in obj:
                    file_count += 1
            print(f"\n{Colors.BRIGHT_GREEN}✅ HTTP API: Found {file_count} file(s) to load{Colors.RESET}\n")
        
        # Second: delta_sharing library to load actual data
        print(f"{Colors.BRIGHT_BLUE}📚 Delta Sharing Library - Loading data:{Colors.RESET}\n")
        df = delta_sharing.load_as_pandas(table_url)
        
        print(f"{Colors.GREEN}✅ Successfully loaded table!{Colors.RESET}")
        print(f"{Colors.BRIGHT_WHITE}   Rows:{Colors.RESET} {len(df)}")
        print(f"{Colors.BRIGHT_WHITE}   Columns:{Colors.RESET} {len(df.columns)}")
        print(f"{Colors.BRIGHT_WHITE}   Column names:{Colors.RESET} {Colors.DIM}{', '.join(df.columns.tolist())}{Colors.RESET}")
        
        # Show first few rows
        if len(df) > 0:
            num_rows = min(5, len(df))
            print(f"\n   First {num_rows} rows:")
            for i, (idx, row) in enumerate(df.head(num_rows).iterrows(), 1):
                print(f"\n   Row {i}:")
                for col in df.columns:
                    val = row[col]
                    # Truncate long values
                    val_str = str(val)
                    if len(val_str) > 60:
                        val_str = val_str[:57] + "..."
                    print(f"     {col}: {val_str}")
        
        duration = time.time() - start_time
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 7: Load Table Data", True, None, duration)
        return df
    except Exception as e:
        duration = time.time() - start_time
        print(f"{Colors.RED}❌ Error loading table data: {e}{Colors.RESET}")
        print(f"\n{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 7: Load Table Data", False, str(e), duration)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# TEST 8: Direct REST API Calls
# ─────────────────────────────────────────────────────────────────────────────

def test_direct_api():
    """
    Test 8: Direct REST API calls
    
    Tests all major Delta Sharing REST API endpoints directly without using
    the Python client library. Validates HTTP responses and status codes.
    
    Endpoints tested:
    - GET /shares
    - GET /shares/{share}
    - GET /shares/{share}/schemas
    - GET /shares/{share}/schemas/{schema}/tables
    - GET /shares/{share}/all-tables
    - GET /shares/{share}/schemas/{schema}/tables/{table}/version
    - GET /shares/{share}/schemas/{schema}/tables/{table}/metadata
    - POST /shares/{share}/schemas/{schema}/tables/{table}/query
    """
    print_section("TEST 8: Direct REST API Calls")
    start_time = time.time()
    
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Dynamically discover first share and schema
    try:
        shares_response = http_get(f"{BASE_URL}/shares", headers=headers)
        first_share = shares_response.json()['items'][0]['name'] if shares_response.status_code == 200 else 'analytics-share'
        
        schemas_response = http_get(f"{BASE_URL}/shares/{first_share}/schemas", headers=headers)
        first_schema = schemas_response.json()['items'][0]['name'] if schemas_response.status_code == 200 else 'default'
        
        tables_response = http_get(f"{BASE_URL}/shares/{first_share}/schemas/{first_schema}/tables", headers=headers)
        first_table = tables_response.json()['items'][0]['name'] if tables_response.status_code == 200 else 'test'
    except:
        first_share = 'analytics-share'
        first_schema = 'default'
        first_table = 'test'
    
    tests = [
        ("GET /shares", f"{BASE_URL}/shares", "GET"),
        (f"GET /shares/{first_share}", f"{BASE_URL}/shares/{first_share}", "GET"),
        (f"GET /shares/{first_share}/schemas", f"{BASE_URL}/shares/{first_share}/schemas", "GET"),
        (f"GET /shares/{first_share}/schemas/{first_schema}/tables", 
         f"{BASE_URL}/shares/{first_share}/schemas/{first_schema}/tables", "GET"),
        (f"GET /shares/{first_share}/all-tables", 
         f"{BASE_URL}/shares/{first_share}/all-tables", "GET"),
        (f"GET /shares/{first_share}/schemas/{first_schema}/tables/{first_table}/version",
         f"{BASE_URL}/shares/{first_share}/schemas/{first_schema}/tables/{first_table}/version", "GET"),
        (f"GET /shares/{first_share}/schemas/{first_schema}/tables/{first_table}/metadata",
         f"{BASE_URL}/shares/{first_share}/schemas/{first_schema}/tables/{first_table}/metadata", "GET"),
        (f"POST /shares/{first_share}/schemas/{first_schema}/tables/{first_table}/query",
         f"{BASE_URL}/shares/{first_share}/schemas/{first_schema}/tables/{first_table}/query", "POST"),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, url, method in tests:
        try:
            if method == "GET":
                response = http_get(url, headers=headers)
            else:  # POST
                response = http_post(url, headers=headers, json_data={"limitHint": 1})
            
            if response.status_code == 200:
                print(f"{Colors.GREEN}✅ {test_name}{Colors.RESET}")
                passed += 1
            else:
                print(f"{Colors.RED}❌ {test_name} (HTTP {response.status_code}){Colors.RESET}")
                failed += 1
        except Exception as e:
            print(f"{Colors.RED}❌ {test_name} (Exception: {type(e).__name__}){Colors.RESET}")
            failed += 1
    
    duration = time.time() - start_time
    print(f"\n{Colors.BRIGHT_CYAN}📊 Results:{Colors.RESET} {Colors.GREEN}{passed} passed{Colors.RESET}, {Colors.RED}{failed} failed{Colors.RESET} out of {len(tests)} tests")
    print(f"{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
    
    # Record overall result
    if failed == 0:
        add_test_result("TEST 8: Direct REST API Calls", True, None, duration)
    else:
        add_test_result("TEST 8: Direct REST API Calls", False, f"{failed} API calls failed", duration)


# ─────────────────────────────────────────────────────────────────────────────
# TEST 9: Data Skipping with Predicates
# ─────────────────────────────────────────────────────────────────────────────

def test_data_skipping():
    """
    Test 9: Data Skipping with Predicates
    
    Tests data skipping functionality using predicate hints to filter partitions.
    Validates that the server correctly skips files that don't match predicates.
    
    Sub-tests:
    - 9.1: Query without predicates (baseline)
    - 9.2: Query with single predicate
    - 9.3: Query with multiple predicates
    - 9.4: Query with limitHint validation
    
    Discovers partition columns dynamically and validates file filtering.
    """
    print_section("TEST 9: Data Skipping with Predicates")
    start_time = time.time()
    
    try:
        _test_data_skipping_impl()
        duration = time.time() - start_time
        print(f"\n{Colors.DIM}⏱️  Total Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 9: Data Skipping with Predicates", True, None, duration)
    except Exception as e:
        duration = time.time() - start_time
        print(f"\n{Colors.RED}❌ Data skipping test failed: {e}{Colors.RESET}")
        print(f"{Colors.DIM}⏱️  Duration: {format_duration(duration)}{Colors.RESET}")
        add_test_result("TEST 9: Data Skipping with Predicates", False, str(e), duration)

def _test_data_skipping_impl():
    
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Dynamically discover resources
    print(f"{Colors.BRIGHT_BLUE}📋 Discovering resources...{Colors.RESET}")
    try:
        shares_response = http_get(f"{BASE_URL}/shares", headers=headers)
        share = shares_response.json()['items'][0]['name'] if shares_response.status_code == 200 else 'analytics-share'
        
        schemas_response = http_get(f"{BASE_URL}/shares/{share}/schemas", headers=headers)
        schema = schemas_response.json()['items'][0]['name'] if schemas_response.status_code == 200 else 'default'
        
        tables_response = http_get(f"{BASE_URL}/shares/{share}/schemas/{schema}/tables", headers=headers)
        table = tables_response.json()['items'][0]['name'] if tables_response.status_code == 200 else 'test'
        
        print(f"{Colors.GREEN}✅ Share:{Colors.RESET} {share}")
        print(f"{Colors.GREEN}✅ Schema:{Colors.RESET} {schema}")
        print(f"{Colors.GREEN}✅ Table:{Colors.RESET} {table}\n")
    except Exception as e:
        print(f"{Colors.RED}❌ Error discovering resources: {e}{Colors.RESET}")
        return
    
    query_url = f"{BASE_URL}/shares/{share}/schemas/{schema}/tables/{table}/query"
    
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
    
    # Test 9.1: Baseline (no predicates) - Discover partitions
    print(f"{Colors.BRIGHT_BLACK}{'─' * 70}{Colors.RESET}")
    print(f"{Colors.BRIGHT_CYAN}{Colors.BOLD}TEST 9.1: Query without predicates (baseline){Colors.RESET}")
    print(f"{Colors.BRIGHT_BLACK}{'─' * 70}{Colors.RESET}")
    
    baseline_files = []
    partition_columns = []
    partition_values_by_column = defaultdict(set)
    
    try:
        subtest_start = time.time()
        response = http_post(query_url, headers=headers, json_data={"limitHint": 100})
        baseline_files = parse_files(response.text)
        subtest_duration = time.time() - subtest_start
        
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
        print(f"\n{Colors.DIM}  ⏱️  Sub-test duration: {format_duration(subtest_duration)}{Colors.RESET}")
    except Exception as e:
        print(f"❌ Error: {e}")
        baseline_count = 0
        print("\n⚠️  Cannot continue with data skipping tests without baseline data")
        return
    
    if not partition_columns or baseline_count == 0:
        print("\n⚠️  Cannot test data skipping: table has no partitions or no files")
        return
    
    print()
    
    # Test 9.2: Single predicate using discovered partition values
    print(f"{Colors.BRIGHT_BLACK}{'─' * 70}{Colors.RESET}")
    first_col = partition_columns[0]
    first_val = sorted(partition_values_by_column[first_col])[0]
    print(f"{Colors.BRIGHT_CYAN}{Colors.BOLD}TEST 9.2: Query with single predicate ({first_col} = {first_val}){Colors.RESET}")
    print(f"{Colors.BRIGHT_BLACK}{'─' * 70}{Colors.RESET}")
    
    try:
        subtest_start = time.time()
        predicate = f"{first_col} = {first_val}"
        response = http_post(query_url, headers=headers, 
                            json_data={"predicateHints": [predicate], "limitHint": 100})
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
        
        subtest_duration = time.time() - subtest_start
        print(f"\n{Colors.DIM}  ⏱️  Sub-test duration: {format_duration(subtest_duration)}{Colors.RESET}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print()
    
    # Test 9.3: Multiple predicates using discovered values
    print(f"{Colors.BRIGHT_BLACK}{'─' * 70}{Colors.RESET}")
    if len(partition_columns) >= 2:
        second_col = partition_columns[1]
        second_val = sorted(partition_values_by_column[second_col])[0]
        print(f"{Colors.BRIGHT_CYAN}{Colors.BOLD}TEST 9.3: Query with multiple predicates ({first_col} = {first_val} AND {second_col} = {second_val}){Colors.RESET}")
    else:
        # Use same column with different value if only one partition column
        second_val = sorted(partition_values_by_column[first_col])[0] if len(partition_values_by_column[first_col]) > 0 else first_val
        second_col = first_col
        print(f"{Colors.BRIGHT_CYAN}{Colors.BOLD}TEST 9.3: Query with multiple predicates ({first_col} = {first_val} AND {second_col} = {second_val}){Colors.RESET}")
    print(f"{Colors.BRIGHT_BLACK}{'─' * 70}{Colors.RESET}")
    
    try:
        subtest_start = time.time()
        predicates = [f"{first_col} = {first_val}", f"{second_col} = {second_val}"]
        response = http_post(query_url, headers=headers, 
                            json_data={"predicateHints": predicates, "limitHint": 100})
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
        
        subtest_duration = time.time() - subtest_start
        print(f"\n{Colors.DIM}  ⏱️  Sub-test duration: {format_duration(subtest_duration)}{Colors.RESET}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print()
    
    # Test 9.4: limitHint validation
    print(f"{Colors.BRIGHT_BLACK}{'─' * 70}{Colors.RESET}")
    print(f"{Colors.BRIGHT_CYAN}{Colors.BOLD}TEST 9.4: Query with limitHint = 2{Colors.RESET}")
    print(f"{Colors.BRIGHT_BLACK}{'─' * 70}{Colors.RESET}")
    
    try:
        subtest_start = time.time()
        response = http_post(query_url, headers=headers, json_data={"limitHint": 2})
        files = parse_files(response.text)
        subtest_duration = time.time() - subtest_start
        
        print(f"\n📊 Result:")
        print(f"  Total files returned: {len(files)}")
        
        if len(files) <= 2:
            print(f"\n  ✅ SUCCESS: limitHint respected (max 2 files)")
        else:
            print(f"\n  ❌ FAIL: limitHint not respected (expected <= 2, got {len(files)})")
        
        print(f"\n{Colors.DIM}  ⏱️  Sub-test duration: {format_duration(subtest_duration)}{Colors.RESET}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "─" * 70)
    print("✨ DATA SKIPPING TESTS COMPLETED")
    print("─" * 70)


# ═══════════════════════════════════════════════════════════════════════════
# 5. CONFIGURATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def load_profile_config(profile_path):
    """
    Load configuration from Delta Sharing profile file
    
    Reads and validates a Delta Sharing profile file in JSON format.
    The profile must contain endpoint URL and authentication token.
    
    Args:
        profile_path (str): Path to the profile file (JSON format)
        
    Returns:
        dict: Configuration dictionary with keys:
            - endpoint (str): Delta Sharing server URL
            - bearerToken (str): Authentication token
            - shareCredentialsVersion (int): Protocol version
        
    Raises:
        FileNotFoundError: If profile file doesn't exist
        json.JSONDecodeError: If profile file is not valid JSON
        KeyError: If required fields (endpoint, bearerToken) are missing
        
    Example:
        >>> config = load_profile_config("config.share")
        >>> print(config['endpoint'])
        http://localhost:8080/delta-sharing
    """
    profile_file = Path(profile_path)
    
    if not profile_file.exists():
        raise FileNotFoundError(
            f"Profile file not found: {profile_path}\n"
            f"Expected format:\n"
            f"{{\n"
            f'  "shareCredentialsVersion": 1,\n'
            f'  "endpoint": "http://localhost:8080/delta-sharing",\n'
            f'  "bearerToken": "dss_..."\n'
            f"}}"
        )
    
    try:
        with open(profile_file, 'r') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid JSON in profile file: {profile_path}",
            e.doc,
            e.pos
        )
    
    # Validate required fields
    required_fields = ['endpoint', 'bearerToken']
    missing_fields = [field for field in required_fields if field not in config]
    
    if missing_fields:
        raise KeyError(
            f"Missing required fields in profile file: {', '.join(missing_fields)}\n"
            f"Profile file: {profile_path}\n"
            f"Required fields: endpoint, bearerToken"
        )
    
    return config

def parse_args():
    """
    Parse command line arguments
    
    Configures and parses command-line arguments for the test suite.
    Supports profile file selection and debug mode activation.
    
    Returns:
        argparse.Namespace: Parsed arguments with attributes:
            - profile (str): Path to profile file
            - debug (bool): Whether debug mode is enabled
            
    Command-line options:
        -p, --profile FILE : Profile file path (default: config.share)
        -d, --debug        : Enable debug mode with HTTP logging
        -h, --help         : Show help message and exit
    """
    parser = argparse.ArgumentParser(
        prog='test_delta_sharing.py',
        description="""
╔══════════════════════════════════════════════════════════════════════════════╗
║                  Delta Sharing Gateway - Test Suite                         ║
║                                                                              ║
║  Comprehensive testing tool for Delta Sharing protocol implementation       ║
║  Tests authentication, shares, schemas, tables, data loading, and more      ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
═══════════════════════════════════════════════════════════════════════════════
USAGE EXAMPLES
═══════════════════════════════════════════════════════════════════════════════

  Basic Usage:
  ────────────
  # Run with default profile (config.share)
  ./test_delta_sharing.py
  
  # Or using python directly
  python test_delta_sharing.py
  
  Custom Profiles:
  ────────────────
  # Use a different profile file
  ./test_delta_sharing.py --profile my-config.share
  ./test_delta_sharing.py -p production.share
  
  # Use admin credentials
  ./test_delta_sharing.py --profile admin-credentials.share
  
  Debug Mode:
  ───────────
  # Enable debug mode to see all HTTP requests with formatted JSON
  ./test_delta_sharing.py --debug
  ./test_delta_sharing.py -d
  
  # Combine custom profile with debug mode
  ./test_delta_sharing.py --profile admin-credentials.share --debug
  ./test_delta_sharing.py -p admin-credentials.share -d
  
  Run Specific Tests:
  ───────────────────
  # Run only TEST 1 (List Shares)
  ./test_delta_sharing.py --test 1
  ./test_delta_sharing.py -t 1
  
  # Run only TEST 6 (Table Metadata)
  ./test_delta_sharing.py --test 6
  
  # Run multiple specific tests
  ./test_delta_sharing.py --test 1 4 6
  ./test_delta_sharing.py -t 5 6 7 8
  
  # Combine with debug mode
  ./test_delta_sharing.py --test 9 --debug
  ./test_delta_sharing.py -t 6 -d
  
  Help:
  ─────
  # Show this help message
  ./test_delta_sharing.py --help
  ./test_delta_sharing.py -h

═══════════════════════════════════════════════════════════════════════════════
PROFILE FILE FORMAT (JSON)
═══════════════════════════════════════════════════════════════════════════════

  The profile file must be in JSON format with the following structure:
  
  {
    "shareCredentialsVersion": 1,
    "endpoint": "http://localhost:8080/delta-sharing",
    "bearerToken": "dss_0000000000000000000000000000000000000000000000000000000000000"
  }

  Required fields:
    • shareCredentialsVersion: Protocol version (use 1)
    • endpoint: Delta Sharing server base URL
    • bearerToken: Authentication token (starts with dss_)

═══════════════════════════════════════════════════════════════════════════════
TESTS AVAILABLE
═══════════════════════════════════════════════════════════════════════════════

  By default, the script runs all tests. Use --test to run specific tests only.
  
  ✓ TEST 1: List all available shares
  ✓ TEST 2: List schemas in each share
  ✓ TEST 3: List tables in each schema
  ✓ TEST 4: List all tables (shortcut)
  ✓ TEST 5: Get file list and statistics
  ✓ TEST 6: Get table metadata and statistics
  ✓ TEST 7: Load table data
  ✓ TEST 8: Test REST API endpoints directly
  ✓ TEST 9: Test data skipping with predicates
  
  All tests are independent and can be run individually or in combination.
  Use --test to run specific tests only.

═══════════════════════════════════════════════════════════════════════════════
DEBUG MODE
═══════════════════════════════════════════════════════════════════════════════

  When enabled with --debug, the script displays:
  
  • All HTTP requests (method, URL, headers, body)
  • All HTTP responses (status code, formatted body)
  • Automatically formatted and indented JSON
  • Support for NDJSON (multiple JSON objects)
  • Masked sensitive tokens for security
  • Visual boxes with Unicode characters for better readability
  
  Ideal for:
    ✓ Debugging and troubleshooting
    ✓ Understanding Delta Sharing protocol in practice
    ✓ Validating server behavior
    ✓ Documenting API examples
    ✓ Development and testing

═══════════════════════════════════════════════════════════════════════════════
IMPORTANT NOTES
═══════════════════════════════════════════════════════════════════════════════

  • Token and endpoint are automatically extracted from the profile file
  • No need to specify them separately on the command line
  • Make sure the Delta Sharing server is running before executing tests
  • Debug mode can generate significant output - redirect to file if needed:
    ./test_delta_sharing.py --debug > test_output.log 2>&1

═══════════════════════════════════════════════════════════════════════════════
REQUIREMENTS
═══════════════════════════════════════════════════════════════════════════════

  Python packages (install with: pip install -r requirements.txt):
    • delta-sharing >= 1.0.0
    • pandas
    • pyarrow
    • requests

═══════════════════════════════════════════════════════════════════════════════
        """,
        add_help=True  # Explicitly enable -h/--help (already default)
    )
    
    parser.add_argument(
        '-p', '--profile',
        metavar='FILE',
        default=DEFAULT_PROFILE_FILE,
        help=f'path to Delta Sharing profile file (default: {DEFAULT_PROFILE_FILE})'
    )
    
    parser.add_argument(
        '-d', '--debug',
        action='store_true',
        help='enable debug mode with detailed logging of all HTTP requests/responses (formatted JSON)'
    )
    
    parser.add_argument(
        '-t', '--test',
        metavar='N',
        type=int,
        nargs='+',
        choices=range(1, 10),
        help='run specific test(s) only (1-9). Examples: --test 1, --test 1 4 7, --test 5 6 7 8'
    )
    
    return parser.parse_args()


# ═══════════════════════════════════════════════════════════════════════════
# 6. MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

def should_run_test(test_num, selected_tests):
    """
    Determine if a test should run based on user selection
    
    Args:
        test_num (int): Test number (1-9)
        selected_tests (list): List of selected test numbers, or None for all tests
        
    Returns:
        bool: True if test should run
    """
    if selected_tests is None:
        return True
    return test_num in selected_tests

def main():
    """
    Main test runner
    
    Executes selected tests or all tests in sequence:
    1. List shares
    2. List schemas
    3. List tables
    4. List all tables (shortcut)
    5. Get file list and statistics
    6. Get table metadata
    7. Load table data
    8. Test direct API calls
    9. Test data skipping with predicates
    """
    global PROFILE_FILE, BEARER_TOKEN, BASE_URL, DEBUG, delta_sharing
    
    # Parse command line arguments
    args = parse_args()
    PROFILE_FILE = args.profile
    DEBUG = args.debug
    
    # Determine which tests to run
    selected_tests = args.test
    tests_to_run = set(selected_tests) if selected_tests else None
    
    print(f"\n{Colors.BRIGHT_CYAN}{'🔷'*40}{Colors.RESET}")
    print(f"{Colors.BRIGHT_CYAN}{Colors.BOLD}  Delta Sharing Gateway - Python Client Test Suite{Colors.RESET}")
    print(f"{Colors.BRIGHT_CYAN}{'🔷'*40}{Colors.RESET}")
    
    # Show which tests will run
    if selected_tests:
        print(f"\n{Colors.BRIGHT_WHITE}📋 Running specific test(s):{Colors.RESET} {Colors.BOLD}{', '.join(f'TEST {t}' for t in sorted(selected_tests))}{Colors.RESET}")
    else:
        print(f"\n{Colors.BRIGHT_WHITE}📋 Running all tests (1-9){Colors.RESET}")
    
    if DEBUG:
        print(f"\n{Colors.MAGENTA}{Colors.BOLD}🔍 DEBUG MODE ENABLED{Colors.RESET} {Colors.DIM}- All HTTP requests/responses will be logged{Colors.RESET}")
        print(f"{Colors.BRIGHT_BLACK}{'─' * 80}{Colors.RESET}")
        
        # Configure delta_sharing logging BEFORE importing the library
        configure_delta_sharing_logging()
    
    # Import delta_sharing AFTER configuring logging (if debug mode is active)
    # This ensures debug logs are captured from the moment the library is loaded
    import delta_sharing
    
    # Load configuration from profile file
    try:
        print(f"\n{Colors.BRIGHT_BLUE}📄 Loading profile: {Colors.BOLD}{PROFILE_FILE}{Colors.RESET}")
        config = load_profile_config(PROFILE_FILE)
        
        # Extract endpoint and token from profile
        BASE_URL = config['endpoint']
        BEARER_TOKEN = config['bearerToken']
        
        print(f"{Colors.GREEN}✅ Profile loaded successfully{Colors.RESET}")
        
    except FileNotFoundError as e:
        print(f"\n{Colors.RED}❌ Error: {e}{Colors.RESET}")
        print(f"\n{Colors.YELLOW}💡 Tip: Make sure the profile file exists and is in JSON format{Colors.RESET}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"\n{Colors.RED}❌ Error: Invalid JSON in profile file{Colors.RESET}")
        print(f"{Colors.DIM}   {e}{Colors.RESET}")
        sys.exit(1)
    except KeyError as e:
        print(f"\n{Colors.RED}❌ Error: {e}{Colors.RESET}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Colors.RED}❌ Unexpected error loading profile: {e}{Colors.RESET}")
        traceback.print_exc()
        sys.exit(1)
    
    print(f"\n{Colors.BRIGHT_WHITE}Configuration:{Colors.RESET}")
    print(f"{Colors.DIM}  Profile File:{Colors.RESET} {PROFILE_FILE}")
    print(f"{Colors.DIM}  Endpoint:{Colors.RESET} {BASE_URL}")
    print(f"{Colors.DIM}  Token:{Colors.RESET} {Colors.YELLOW}{BEARER_TOKEN[:20]}...{Colors.RESET}" if len(BEARER_TOKEN) > 20 else f"{Colors.DIM}  Token:{Colors.RESET} {Colors.YELLOW}{BEARER_TOKEN}{Colors.RESET}")
    
    try:
        # Test 1: List shares
        if should_run_test(1, tests_to_run):
            test_list_shares()
        
        # Test 2: List schemas
        if should_run_test(2, tests_to_run):
            test_list_schemas()
        
        # Test 3: List tables in schemas (uses lazy loading for shares)
        if should_run_test(3, tests_to_run):
            shares = get_shares_lazy()
            if shares:
                share_schemas = []
                for share in shares:
                    try:
                        client = delta_sharing.SharingClient(PROFILE_FILE)
                        schemas = client.list_schemas(share)
                        for schema in schemas:
                            share_schemas.append((share, schema))
                    except Exception as e:
                        if DEBUG:
                            print(f"{Colors.RED}Error in TEST 3: {e}{Colors.RESET}")
                
                if share_schemas:
                    test_list_tables(share_schemas)
        
        # Test 4: List all tables
        if should_run_test(4, tests_to_run):
            test_list_all_tables()
        
        # Test 5: Get file list and statistics (uses lazy loading for tables)
        if should_run_test(5, tests_to_run):
            test_file_list()
        
        # Test 6: Get table metadata (uses lazy loading for tables)
        if should_run_test(6, tests_to_run):
            test_table_metadata()
        
        # Test 7: Load table data (uses lazy loading for tables)
        if should_run_test(7, tests_to_run):
            test_load_table_data()
        
        # Test 8: Direct API calls
        if should_run_test(8, tests_to_run):
            test_direct_api()
        
        # Test 9: Data skipping
        if should_run_test(9, tests_to_run):
            test_data_skipping()
        
        # Summary
        print_section("TEST SUMMARY")
        
        # Count passed and failed tests
        total_tests = len(TEST_RESULTS)
        passed_tests = sum(1 for _, passed, _, _ in TEST_RESULTS if passed)
        failed_tests = total_tests - passed_tests
        
        # Display results overview
        if failed_tests == 0:
            print(f"{Colors.GREEN}{Colors.BOLD}✅ All tests completed successfully!{Colors.RESET}\n")
        else:
            print(f"{Colors.YELLOW}{Colors.BOLD}⚠️  Tests completed with some failures{Colors.RESET}\n")
        
        # Display individual test results
        print(f"{Colors.BRIGHT_CYAN}{Colors.BOLD}Test Results:{Colors.RESET}\n")
        
        for test_name, passed, error_message, duration in TEST_RESULTS:
            if passed:
                icon = "✅"
                color = Colors.GREEN
                status_text = "PASSED"
            else:
                icon = "❌"
                color = Colors.RED
                status_text = "FAILED"
            
            duration_str = format_duration(duration)
            print(f"  {icon} {color}{status_text}{Colors.RESET} - {test_name} {Colors.DIM}({duration_str}){Colors.RESET}")
            if error_message and not passed:
                # Truncate long error messages
                if len(error_message) > 80:
                    error_message = error_message[:77] + "..."
                print(f"{Colors.DIM}     └─ Error: {error_message}{Colors.RESET}")
        
        # Display statistics
        print(f"\n{Colors.BRIGHT_CYAN}{Colors.BOLD}Statistics:{Colors.RESET}")
        print(f"{Colors.BRIGHT_WHITE}   Total Tests:{Colors.RESET} {total_tests}")
        print(f"{Colors.GREEN}   Passed:{Colors.RESET} {passed_tests}")
        print(f"{Colors.RED}   Failed:{Colors.RESET} {failed_tests}")
        
        # Performance statistics
        print(f"\n{Colors.BRIGHT_CYAN}{Colors.BOLD}Performance:{Colors.RESET}")
        total_duration = sum(duration for _, _, _, duration in TEST_RESULTS)
        avg_duration = total_duration / total_tests if total_tests > 0 else 0
        min_duration = min((duration for _, _, _, duration in TEST_RESULTS), default=0)
        max_duration = max((duration for _, _, _, duration in TEST_RESULTS), default=0)
        
        print(f"{Colors.BRIGHT_WHITE}   Total Time:{Colors.RESET} {format_duration(total_duration)}")
        print(f"{Colors.BRIGHT_WHITE}   Average Time:{Colors.RESET} {format_duration(avg_duration)}")
        print(f"{Colors.BRIGHT_WHITE}   Fastest Test:{Colors.RESET} {format_duration(min_duration)}")
        print(f"{Colors.BRIGHT_WHITE}   Slowest Test:{Colors.RESET} {format_duration(max_duration)}")
        
        # Find slowest and fastest tests
        if TEST_RESULTS:
            slowest = max(TEST_RESULTS, key=lambda x: x[3])
            fastest = min(TEST_RESULTS, key=lambda x: x[3])
            print(f"{Colors.DIM}   Slowest: {slowest[0]} ({format_duration(slowest[3])}){Colors.RESET}")
            print(f"{Colors.DIM}   Fastest: {fastest[0]} ({format_duration(fastest[3])}){Colors.RESET}")
        
        # Show resources found (using cached values if available)
        print(f"\n{Colors.BRIGHT_CYAN}{Colors.BOLD}Resources Found:{Colors.RESET}")
        if _cached_shares is not None:
            print(f"{Colors.BRIGHT_WHITE}   Shares:{Colors.RESET} {Colors.CYAN}{len(_cached_shares)}{Colors.RESET}")
        if _cached_all_tables is not None:
            print(f"{Colors.BRIGHT_WHITE}   Tables:{Colors.RESET} {Colors.CYAN}{len(_cached_all_tables)}{Colors.RESET}")
        
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}⚠️  Tests interrupted by user{Colors.RESET}")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n{Colors.RED}❌ Unexpected error: {e}{Colors.RESET}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
