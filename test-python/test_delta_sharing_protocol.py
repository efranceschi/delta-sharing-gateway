#!/usr/bin/env python3
"""
Delta Sharing Protocol Test Suite

This script tests all endpoints of a Delta Sharing server according to the
official Delta Sharing Protocol specification:
https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md

Usage:
    python test_delta_sharing_protocol.py <config.share>
    
Example:
    python test_delta_sharing_protocol.py config.share
"""

import sys
import json
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime
import argparse


class Colors:
    """ANSI color codes for terminal output"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class DeltaSharingTester:
    """Test suite for Delta Sharing Protocol endpoints"""
    
    def __init__(self, config_file: str):
        """Initialize tester with configuration file"""
        self.config = self._load_config(config_file)
        self.endpoint = self.config['endpoint']
        self.token = self.config['bearerToken']
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        self.test_count = 0
        self.success_count = 0
        self.fail_count = 0
        
        # Store discovered resources for subsequent tests
        self.discovered_share = None
        self.discovered_schema = None
        self.discovered_table = None
        self.discovered_shares = []
        self.discovered_tables = []
        
        # Store individual test results for detailed summary
        self.test_results = []
        
        # Flag to track if initialization was successful
        self.initialized = False
        
        # Available tests mapping
        self.available_tests = {
            '1': ('test_1_list_shares', 'List Shares'),
            '1.1': ('test_1_1_list_shares_paginated', 'List Shares (Paginated)'),
            '2': ('test_2_get_share', 'Get Share'),
            '3': ('test_3_list_schemas', 'List Schemas'),
            '3.1': ('test_3_1_list_schemas_paginated', 'List Schemas (Paginated)'),
            '4': ('test_4_list_tables', 'List Tables'),
            '4.1': ('test_4_1_list_tables_paginated', 'List Tables (Paginated)'),
            '5': ('test_5_list_all_tables', 'List All Tables'),
            '5.1': ('test_5_1_list_all_tables_paginated', 'List All Tables (Paginated)'),
            '6': ('test_6_query_table_version', 'Query Table Version'),
            '7': ('test_7_query_table_metadata', 'Query Table Metadata (Basic)'),
            '7.1': ('test_7_1_query_table_metadata_parquet', 'Query Table Metadata (Parquet format)'),
            '7.2': ('test_7_2_query_table_metadata_delta', 'Query Table Metadata (Delta format)'),
            '7.3': ('test_7_3_query_table_metadata_with_end_stream_action_parquet', 'Query Table Metadata (Parquet + EndStreamAction)'),
            '7.4': ('test_7_4_query_table_metadata_with_end_stream_action_delta', 'Query Table Metadata (Delta + EndStreamAction)'),
            '7.5': ('test_7_5_query_table_metadata_delta_with_readerfeatures', 'Query Table Metadata (Delta + readerfeatures)'),
            '7.6': ('test_7_6_query_table_metadata_delta_with_deletion_vectors', 'Query Table Metadata (Delta + deletionvectors)'),
            '7.7': ('test_7_7_query_table_metadata_delta_with_column_mapping', 'Query Table Metadata (Delta + columnmapping)'),
            '7.8': ('test_7_8_query_table_metadata_delta_with_timestampntz', 'Query Table Metadata (Delta + timestampntz)'),
            '8': ('test_8_query_table_data_basic', 'Query Table Data (Basic)'),
            '8.1': ('test_8_1_query_table_data_parquet', 'Query Table Data (Parquet format)'),
            '8.2': ('test_8_2_query_table_data_delta', 'Query Table Data (Delta format)'),
            '8.3': ('test_8_3_query_table_data_with_limit_parquet', 'Query Table Data (Parquet + limitHint)'),
            '8.4': ('test_8_4_query_table_data_with_limit_delta', 'Query Table Data (Delta + limitHint)'),
            '8.5': ('test_8_5_query_table_data_with_version_parquet', 'Query Table Data (Parquet + version)'),
            '8.6': ('test_8_6_query_table_data_with_version_delta', 'Query Table Data (Delta + version)'),
            '8.7': ('test_8_7_query_table_data_with_end_stream_action_parquet', 'Query Table Data (Parquet + EndStreamAction)'),
            '8.8': ('test_8_8_query_table_data_with_end_stream_action_delta', 'Query Table Data (Delta + EndStreamAction)'),
            '8.9': ('test_8_9_query_table_data_with_predicates_parquet', 'Query Table Data (Parquet + predicateHints)'),
            '8.10': ('test_8_10_query_table_data_with_predicates_delta', 'Query Table Data (Delta + predicateHints)'),
            '8.11': ('test_8_11_query_table_data_delta_with_readerfeatures', 'Query Table Data (Delta + readerfeatures)'),
            '8.12': ('test_8_12_query_table_data_delta_with_deletion_vectors', 'Query Table Data (Delta + deletionvectors)'),
            '8.13': ('test_8_13_query_table_data_delta_with_column_mapping', 'Query Table Data (Delta + columnmapping)'),
            '8.14': ('test_8_14_query_table_data_delta_with_timestampntz', 'Query Table Data (Delta + timestampntz)'),
            '9': ('test_9_query_table_changes_parquet', 'Query Table Changes (CDF - Parquet format)'),
            '9.1': ('test_9_1_query_table_changes_delta', 'Query Table Changes (CDF - Delta format)'),
        }
        
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load Delta Sharing configuration file"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            return config
        except Exception as e:
            print(f"{Colors.FAIL}Error loading config file: {e}{Colors.ENDC}")
            sys.exit(1)
    
    def _format_json(self, data: Any) -> str:
        """Format JSON with 2-space indentation"""
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except:
                return data
        return json.dumps(data, indent=2, ensure_ascii=False)
    
    def _format_headers(self, headers: Dict[str, str]) -> str:
        """Format headers for display"""
        return '\n'.join([f"  {k}: {v}" for k, v in headers.items()])
    
    def _parse_ndjson(self, response_text: str) -> List[Dict[str, Any]]:
        """Parse newline-delimited JSON response"""
        lines = []
        for line in response_text.strip().split('\n'):
            if line.strip():
                try:
                    lines.append(json.loads(line))
                except json.JSONDecodeError as e:
                    lines.append({'_parse_error': str(e), '_raw': line})
        return lines
    
    def _initialize_resources(self):
        """
        Initialize by discovering shares, schemas, and tables using all-tables endpoint.
        This is a mandatory step before running any tests.
        """
        print(f"\n{Colors.BOLD}{Colors.OKCYAN}Initializing test resources...{Colors.ENDC}\n")
        
        try:
            # Step 1: List shares
            print(f"{Colors.BOLD}Step 1: Discovering shares...{Colors.ENDC}")
            url = f"{self.endpoint}/shares"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"{Colors.FAIL}✗ Failed to list shares: HTTP {response.status_code}{Colors.ENDC}")
                return False
            
            shares_data = response.json()
            if 'items' not in shares_data or len(shares_data['items']) == 0:
                print(f"{Colors.FAIL}✗ No shares found in the system{Colors.ENDC}")
                return False
            
            self.discovered_shares = shares_data['items']
            self.discovered_share = shares_data['items'][0]['name']
            print(f"{Colors.OKGREEN}✓ Found {len(self.discovered_shares)} share(s){Colors.ENDC}")
            print(f"  Using share: {Colors.OKCYAN}{self.discovered_share}{Colors.ENDC}")
            
            # Step 2: Use all-tables endpoint to get schemas and tables
            print(f"\n{Colors.BOLD}Step 2: Discovering schemas and tables via all-tables endpoint...{Colors.ENDC}")
            url = f"{self.endpoint}/shares/{self.discovered_share}/all-tables"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"{Colors.FAIL}✗ Failed to list all tables: HTTP {response.status_code}{Colors.ENDC}")
                return False
            
            tables_data = response.json()
            if 'items' not in tables_data or len(tables_data['items']) == 0:
                print(f"{Colors.FAIL}✗ No tables found in share '{self.discovered_share}'{Colors.ENDC}")
                return False
            
            self.discovered_tables = tables_data['items']
            
            # Extract unique schemas
            schemas = set()
            for table in self.discovered_tables:
                if 'schema' in table:
                    schemas.add(table['schema'])
            
            if len(schemas) == 0:
                print(f"{Colors.FAIL}✗ No schemas found in the tables{Colors.ENDC}")
                return False
            
            # Use first table for testing
            first_table = self.discovered_tables[0]
            self.discovered_schema = first_table.get('schema')
            self.discovered_table = first_table.get('name')
            
            print(f"{Colors.OKGREEN}✓ Found {len(schemas)} schema(s) and {len(self.discovered_tables)} table(s){Colors.ENDC}")
            print(f"  Using schema: {Colors.OKCYAN}{self.discovered_schema}{Colors.ENDC}")
            print(f"  Using table: {Colors.OKCYAN}{self.discovered_table}{Colors.ENDC}")
            
            # Display all discovered resources
            print(f"\n{Colors.BOLD}Discovered Resources Summary:{Colors.ENDC}")
            print(f"  Share: {self.discovered_share}")
            print(f"  Schemas: {', '.join(sorted(schemas))}")
            print(f"  Tables ({len(self.discovered_tables)}):")
            for table in self.discovered_tables[:5]:  # Show first 5
                print(f"    - {table.get('schema')}.{table.get('name')}")
            if len(self.discovered_tables) > 5:
                print(f"    ... and {len(self.discovered_tables) - 5} more")
            
            self.initialized = True
            print(f"\n{Colors.OKGREEN}✓ Initialization completed successfully{Colors.ENDC}")
            return True
            
        except Exception as e:
            print(f"{Colors.FAIL}✗ Initialization failed: {str(e)}{Colors.ENDC}")
            return False
    
    def _print_separator(self, char: str = "="):
        """Print separator line"""
        print(f"\n{char * 80}\n")
    
    def _print_test_header(self, test_number: str, test_name: str, method: str, path: str):
        """Print test header"""
        self._print_separator()
        print(f"{Colors.BOLD}{Colors.OKBLUE}Test {test_number}: {test_name}{Colors.ENDC}")
        print(f"{Colors.OKCYAN}{method} {path}{Colors.ENDC}")
        self._print_separator("-")
    
    def _execute_request(
        self,
        test_number: str,
        test_name: str,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        expect_ndjson: bool = False
    ) -> Optional[Any]:
        """Execute HTTP request and display results"""
        
        self.test_count += 1
        test_start_time = datetime.now()
        self._print_test_header(test_number, test_name, method, path)
        
        # Merge headers
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)
        
        # Build full URL
        url = f"{self.endpoint}{path}"
        
        # Print request details
        print(f"{Colors.BOLD}REQUEST:{Colors.ENDC}")
        print(f"URL: {url}")
        if params:
            print(f"Query Parameters:")
            for key, value in params.items():
                print(f"  {key} = {value}")
        print(f"Headers:")
        print(self._format_headers(request_headers))
        
        if json_body:
            print(f"\nRequest Body:")
            print(self._format_json(json_body))
        
        print(f"\n{Colors.BOLD}EXECUTING...{Colors.ENDC}")
        
        try:
            # Execute request
            start_time = datetime.now()
            
            if method.upper() == 'GET':
                response = requests.get(url, headers=request_headers, params=params)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=request_headers, json=json_body, params=params)
            elif method.upper() == 'HEAD':
                response = requests.head(url, headers=request_headers, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Print response details
            print(f"\n{Colors.BOLD}RESPONSE:{Colors.ENDC}")
            print(f"Status Code: {response.status_code} {response.reason}")
            print(f"Duration: {duration:.3f} seconds")
            print(f"Response Headers:")
            print(self._format_headers(dict(response.headers)))
            
            # Parse and display response body
            response_body = None
            if response.text:
                # Check Content-Type header first to determine the correct format
                # This avoids parsing the entire payload incorrectly
                content_type = response.headers.get('Content-Type', '').lower()
                
                # Determine if response is NDJSON (Newline-Delimited JSON)
                # Check Content-Type header first, fallback to expect_ndjson parameter
                is_ndjson = False
                if 'application/x-ndjson' in content_type:
                    is_ndjson = True
                elif expect_ndjson and 'application/json' not in content_type:
                    # If we expect NDJSON but Content-Type is not explicit, use expect_ndjson
                    is_ndjson = True
                
                format_type = "NDJSON (application/x-ndjson)" if is_ndjson else "JSON"
                print(f"\nResponse Body (Format: {format_type}):")
                
                if is_ndjson:
                    # Parse as NDJSON - each line is a separate JSON object
                    parsed_lines = self._parse_ndjson(response.text)
                    for i, line in enumerate(parsed_lines, 1):
                        print(f"\nLine {i}:")
                        print(self._format_json(line))
                    response_body = parsed_lines
                else:
                    # Parse as regular JSON (single object/array)
                    try:
                        response_body = response.json()
                        print(self._format_json(response_body))
                    except json.JSONDecodeError as e:
                        # If JSON parsing fails, display raw text
                        print(f"(JSON parsing failed: {e})")
                        print(response.text)
                        response_body = response.text
            else:
                print("\n(Empty response body)")
            
            # Check status
            test_duration = (datetime.now() - test_start_time).total_seconds()
            if 200 <= response.status_code < 300:
                print(f"\n{Colors.OKGREEN}✓ TEST PASSED{Colors.ENDC}")
                self.success_count += 1
                self.test_results.append({
                    'number': test_number,
                    'name': test_name,
                    'status': 'PASSED',
                    'status_code': response.status_code,
                    'duration': test_duration
                })
                return response_body
            else:
                print(f"\n{Colors.WARNING}⚠ TEST COMPLETED WITH NON-2XX STATUS{Colors.ENDC}")
                self.fail_count += 1
                self.test_results.append({
                    'number': test_number,
                    'name': test_name,
                    'status': 'WARNING',
                    'status_code': response.status_code,
                    'duration': test_duration
                })
                return response_body
                
        except Exception as e:
            test_duration = (datetime.now() - test_start_time).total_seconds()
            print(f"\n{Colors.FAIL}✗ TEST FAILED: {str(e)}{Colors.ENDC}")
            self.fail_count += 1
            self.test_results.append({
                'number': test_number,
                'name': test_name,
                'status': 'FAILED',
                'error': str(e),
                'duration': test_duration
            })
            return None
    
    def test_1_list_shares(self):
        """Test 1: List Shares"""
        self._execute_request(
            test_number="1",
            test_name="List Shares",
            method="GET",
            path="/shares"
        )
    
    def test_1_1_list_shares_paginated(self):
        """Test 1.1: List Shares with pagination"""
        self._execute_request(
            test_number="1.1",
            test_name="List Shares (Paginated)",
            method="GET",
            path="/shares",
            params={"maxResults": 1}
        )
    
    def test_2_get_share(self):
        """Test 2: Get Share"""
        if not self.discovered_share:
            print(f"{Colors.WARNING}⚠ Skipping test 2: No share discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="2",
            test_name="Get Share",
            method="GET",
            path=f"/shares/{self.discovered_share}"
        )
    
    def test_3_list_schemas(self):
        """Test 3: List Schemas in Share"""
        if not self.discovered_share:
            print(f"{Colors.WARNING}⚠ Skipping test 3: No share discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="3",
            test_name="List Schemas",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas"
        )
    
    def test_3_1_list_schemas_paginated(self):
        """Test 3.1: List Schemas with pagination"""
        if not self.discovered_share:
            print(f"{Colors.WARNING}⚠ Skipping test 3.1: No share discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="3.1",
            test_name="List Schemas (Paginated)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas",
            params={"maxResults": 1}
        )
    
    def test_4_list_tables(self):
        """Test 4: List Tables in Schema"""
        if not self.discovered_share or not self.discovered_schema:
            print(f"{Colors.WARNING}⚠ Skipping test 4: No share/schema discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="4",
            test_name="List Tables in Schema",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables"
        )
    
    def test_4_1_list_tables_paginated(self):
        """Test 4.1: List Tables with pagination"""
        if not self.discovered_share or not self.discovered_schema:
            print(f"{Colors.WARNING}⚠ Skipping test 4.1: No share/schema discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="4.1",
            test_name="List Tables (Paginated)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables",
            params={"maxResults": 1}
        )
    
    def test_5_list_all_tables(self):
        """Test 5: List All Tables in Share"""
        if not self.discovered_share:
            print(f"{Colors.WARNING}⚠ Skipping test 5: No share discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="5",
            test_name="List All Tables",
            method="GET",
            path=f"/shares/{self.discovered_share}/all-tables"
        )
    
    def test_5_1_list_all_tables_paginated(self):
        """Test 5.1: List All Tables with pagination"""
        if not self.discovered_share:
            print(f"{Colors.WARNING}⚠ Skipping test 5.1: No share discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="5.1",
            test_name="List All Tables (Paginated)",
            method="GET",
            path=f"/shares/{self.discovered_share}/all-tables",
            params={"maxResults": 1}
        )
    
    def test_6_query_table_version(self):
        """Test 6: Query Table Version"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 6: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="6",
            test_name="Query Table Version",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/version"
        )
    
    def test_7_query_table_metadata(self):
        """Test 7: Query Table Metadata (Basic)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 7: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="7",
            test_name="Query Table Metadata (Basic)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/metadata",
            expect_ndjson=True
        )
    
    def test_7_1_query_table_metadata_parquet(self):
        """Test 7.1: Query Table Metadata (Parquet format)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 7.1: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="7.1",
            test_name="Query Table Metadata (Parquet format)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/metadata",
            headers={
                "delta-sharing-capabilities": "responseformat=parquet",
                "Delta-Table-Version": "1"
            },
            expect_ndjson=True
        )
    
    def test_7_2_query_table_metadata_delta(self):
        """Test 7.2: Query Table Metadata (Delta format)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 7.2: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="7.2",
            test_name="Query Table Metadata (Delta format)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/metadata",
            headers={
                "delta-sharing-capabilities": "responseformat=delta",
                "Delta-Table-Version": "1"
            },
            expect_ndjson=True
        )
    
    def test_7_3_query_table_metadata_with_end_stream_action_parquet(self):
        """Test 7.3: Query Table Metadata (Parquet + EndStreamAction)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 7.3: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="7.3",
            test_name="Query Table Metadata (Parquet + EndStreamAction)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/metadata",
            headers={
                "delta-sharing-capabilities": "responseformat=parquet",
                "includeEndStreamAction": "true",
                "Delta-Table-Version": "1"
            },
            expect_ndjson=True
        )
    
    def test_7_4_query_table_metadata_with_end_stream_action_delta(self):
        """Test 7.4: Query Table Metadata (Delta + EndStreamAction)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 7.4: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="7.4",
            test_name="Query Table Metadata (Delta + EndStreamAction)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/metadata",
            headers={
                "delta-sharing-capabilities": "responseformat=delta",
                "includeEndStreamAction": "true",
                "Delta-Table-Version": "1"
            },
            expect_ndjson=True
        )
    
    def test_7_5_query_table_metadata_delta_with_readerfeatures(self):
        """Test 7.5: Query Table Metadata (Delta + readerfeatures)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 7.5: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="7.5",
            test_name="Query Table Metadata (Delta + readerfeatures)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/metadata",
            headers={
                "delta-sharing-capabilities": "responseformat=delta;readerfeatures=deletionvectors,columnmapping,timestampntz",
                "Delta-Table-Version": "1"
            },
            expect_ndjson=True
        )
    
    def test_7_6_query_table_metadata_delta_with_deletion_vectors(self):
        """Test 7.6: Query Table Metadata (Delta + deletionvectors)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 7.6: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="7.6",
            test_name="Query Table Metadata (Delta + deletionvectors)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/metadata",
            headers={
                "delta-sharing-capabilities": "responseformat=delta;readerfeatures=deletionvectors",
                "Delta-Table-Version": "1"
            },
            expect_ndjson=True
        )
    
    def test_7_7_query_table_metadata_delta_with_column_mapping(self):
        """Test 7.7: Query Table Metadata (Delta + columnmapping)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 7.7: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="7.7",
            test_name="Query Table Metadata (Delta + columnmapping)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/metadata",
            headers={
                "delta-sharing-capabilities": "responseformat=delta;readerfeatures=columnmapping",
                "Delta-Table-Version": "1"
            },
            expect_ndjson=True
        )
    
    def test_7_8_query_table_metadata_delta_with_timestampntz(self):
        """Test 7.8: Query Table Metadata (Delta + timestampntz)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 7.8: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="7.8",
            test_name="Query Table Metadata (Delta + timestampntz)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/metadata",
            headers={
                "delta-sharing-capabilities": "responseformat=delta;readerfeatures=timestampntz",
                "Delta-Table-Version": "1"
            },
            expect_ndjson=True
        )
    
    def test_8_query_table_data_basic(self):
        """Test 8: Query Table Data (Basic)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8",
            test_name="Query Table Data (Basic)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            json_body={},
            expect_ndjson=True
        )
    
    def test_8_1_query_table_data_parquet(self):
        """Test 8.1: Query Table Data (Parquet format)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.1: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.1",
            test_name="Query Table Data (Parquet format)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=parquet"
            },
            json_body={},
            expect_ndjson=True
        )
    
    def test_8_2_query_table_data_delta(self):
        """Test 8.2: Query Table Data (Delta format)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.2: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.2",
            test_name="Query Table Data (Delta format)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=delta"
            },
            json_body={},
            expect_ndjson=True
        )
    
    def test_8_3_query_table_data_with_limit_parquet(self):
        """Test 8.3: Query Table Data (Parquet + limitHint)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.3: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.3",
            test_name="Query Table Data (Parquet + limitHint)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=parquet"
            },
            json_body={
                "limitHint": 10
            },
            expect_ndjson=True
        )
    
    def test_8_4_query_table_data_with_limit_delta(self):
        """Test 8.4: Query Table Data (Delta + limitHint)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.4: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.4",
            test_name="Query Table Data (Delta + limitHint)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=delta"
            },
            json_body={
                "limitHint": 10
            },
            expect_ndjson=True
        )
    
    def test_8_5_query_table_data_with_version_parquet(self):
        """Test 8.5: Query Table Data (Parquet + version)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.5: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.5",
            test_name="Query Table Data (Parquet + version)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=parquet"
            },
            json_body={
                "version": 0
            },
            expect_ndjson=True
        )
    
    def test_8_6_query_table_data_with_version_delta(self):
        """Test 8.6: Query Table Data (Delta + version)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.6: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.6",
            test_name="Query Table Data (Delta + version)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=delta"
            },
            json_body={
                "version": 0
            },
            expect_ndjson=True
        )
    
    def test_8_7_query_table_data_with_end_stream_action_parquet(self):
        """Test 8.7: Query Table Data (Parquet + EndStreamAction)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.7: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.7",
            test_name="Query Table Data (Parquet + EndStreamAction)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=parquet",
                "includeEndStreamAction": "true",
                "Delta-Table-Version": "1"
            },
            json_body={},
            expect_ndjson=True
        )
    
    def test_8_8_query_table_data_with_end_stream_action_delta(self):
        """Test 8.8: Query Table Data (Delta + EndStreamAction)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.8: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.8",
            test_name="Query Table Data (Delta + EndStreamAction)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=delta",
                "includeEndStreamAction": "true",
                "Delta-Table-Version": "1"
            },
            json_body={},
            expect_ndjson=True
        )
    
    def test_8_9_query_table_data_with_predicates_parquet(self):
        """Test 8.9: Query Table Data (Parquet + predicateHints)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.9: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.9",
            test_name="Query Table Data (Parquet + predicateHints)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=parquet"
            },
            json_body={
                "predicateHints": [
                    "id > 100"
                ]
            },
            expect_ndjson=True
        )
    
    def test_8_10_query_table_data_with_predicates_delta(self):
        """Test 8.10: Query Table Data (Delta + predicateHints)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.10: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.10",
            test_name="Query Table Data (Delta + predicateHints)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=delta"
            },
            json_body={
                "predicateHints": [
                    "id > 100"
                ]
            },
            expect_ndjson=True
        )
    
    def test_8_11_query_table_data_delta_with_readerfeatures(self):
        """Test 8.11: Query Table Data (Delta + readerfeatures)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.11: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.11",
            test_name="Query Table Data (Delta + readerfeatures)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=delta;readerfeatures=deletionvectors,columnmapping,timestampntz"
            },
            json_body={},
            expect_ndjson=True
        )
    
    def test_8_12_query_table_data_delta_with_deletion_vectors(self):
        """Test 8.12: Query Table Data (Delta + deletionvectors)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.12: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.12",
            test_name="Query Table Data (Delta + deletionvectors)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=delta;readerfeatures=deletionvectors"
            },
            json_body={},
            expect_ndjson=True
        )
    
    def test_8_13_query_table_data_delta_with_column_mapping(self):
        """Test 8.13: Query Table Data (Delta + columnmapping)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.13: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.13",
            test_name="Query Table Data (Delta + columnmapping)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=delta;readerfeatures=columnmapping"
            },
            json_body={},
            expect_ndjson=True
        )
    
    def test_8_14_query_table_data_delta_with_timestampntz(self):
        """Test 8.14: Query Table Data (Delta + timestampntz)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 8.14: No share/schema/table discovered{Colors.ENDC}")
            return
        
        self._execute_request(
            test_number="8.14",
            test_name="Query Table Data (Delta + timestampntz)",
            method="POST",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/query",
            headers={
                "delta-sharing-capabilities": "responseformat=delta;readerfeatures=timestampntz"
            },
            json_body={},
            expect_ndjson=True
        )
    
    def test_9_query_table_changes_parquet(self):
        """Test 9: Query Table Changes (CDF - Parquet format)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 9: No share/schema/table discovered{Colors.ENDC}")
            return
        
        # Note: This might fail if CDF is not enabled on the table
        self._execute_request(
            test_number="9",
            test_name="Query Table Changes (CDF - Parquet format)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/changes",
            headers={
                "delta-sharing-capabilities": "responseformat=parquet"
            },
            params={
                "startingVersion": "0"
            },
            expect_ndjson=True
        )
    
    def test_9_1_query_table_changes_delta(self):
        """Test 9.1: Query Table Changes (CDF - Delta format)"""
        if not self.discovered_share or not self.discovered_schema or not self.discovered_table:
            print(f"{Colors.WARNING}⚠ Skipping test 9.1: No share/schema/table discovered{Colors.ENDC}")
            return
        
        # Note: This might fail if CDF is not enabled on the table
        self._execute_request(
            test_number="9.1",
            test_name="Query Table Changes (CDF - Delta format)",
            method="GET",
            path=f"/shares/{self.discovered_share}/schemas/{self.discovered_schema}/tables/{self.discovered_table}/changes",
            headers={
                "delta-sharing-capabilities": "responseformat=delta"
            },
            params={
                "startingVersion": "0"
            },
            expect_ndjson=True
        )
    
    def run_all_tests(self):
        """Run all test cases"""
        print(f"\n{Colors.BOLD}{Colors.HEADER}")
        print("=" * 80)
        print("  DELTA SHARING PROTOCOL TEST SUITE")
        print("=" * 80)
        print(f"{Colors.ENDC}\n")
        print(f"Testing endpoint: {Colors.OKBLUE}{self.endpoint}{Colors.ENDC}")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # MANDATORY: Initialize resources using all-tables endpoint
        if not self._initialize_resources():
            print(f"\n{Colors.FAIL}✗ Initialization failed. Cannot proceed with tests.{Colors.ENDC}")
            print(f"{Colors.WARNING}Please ensure the server is running and has at least one share with tables.{Colors.ENDC}")
            return
        
        # Run tests in sequence
        self.test_1_list_shares()
        self.test_1_1_list_shares_paginated()
        self.test_2_get_share()
        self.test_3_list_schemas()
        self.test_3_1_list_schemas_paginated()
        self.test_4_list_tables()
        self.test_4_1_list_tables_paginated()
        self.test_5_list_all_tables()
        self.test_5_1_list_all_tables_paginated()
        self.test_6_query_table_version()
        self.test_7_query_table_metadata()
        self.test_7_1_query_table_metadata_parquet()
        self.test_7_2_query_table_metadata_delta()
        self.test_7_3_query_table_metadata_with_end_stream_action_parquet()
        self.test_7_4_query_table_metadata_with_end_stream_action_delta()
        self.test_7_5_query_table_metadata_delta_with_readerfeatures()
        self.test_7_6_query_table_metadata_delta_with_deletion_vectors()
        self.test_7_7_query_table_metadata_delta_with_column_mapping()
        self.test_7_8_query_table_metadata_delta_with_timestampntz()
        self.test_8_query_table_data_basic()
        self.test_8_1_query_table_data_parquet()
        self.test_8_2_query_table_data_delta()
        self.test_8_3_query_table_data_with_limit_parquet()
        self.test_8_4_query_table_data_with_limit_delta()
        self.test_8_5_query_table_data_with_version_parquet()
        self.test_8_6_query_table_data_with_version_delta()
        self.test_8_7_query_table_data_with_end_stream_action_parquet()
        self.test_8_8_query_table_data_with_end_stream_action_delta()
        self.test_8_9_query_table_data_with_predicates_parquet()
        self.test_8_10_query_table_data_with_predicates_delta()
        self.test_8_11_query_table_data_delta_with_readerfeatures()
        self.test_8_12_query_table_data_delta_with_deletion_vectors()
        self.test_8_13_query_table_data_delta_with_column_mapping()
        self.test_8_14_query_table_data_delta_with_timestampntz()
        self.test_9_query_table_changes_parquet()
        self.test_9_1_query_table_changes_delta()
        
        # Print detailed summary
        self._print_detailed_summary()
    
    def run_single_test(self, test_id: str):
        """Run a single test by ID"""
        if test_id not in self.available_tests:
            print(f"\n{Colors.FAIL}Error: Test '{test_id}' not found{Colors.ENDC}")
            print(f"\nAvailable tests:")
            self.list_available_tests()
            return
        
        method_name, test_description = self.available_tests[test_id]
        
        print(f"\n{Colors.BOLD}{Colors.HEADER}")
        print("=" * 80)
        print(f"  DELTA SHARING PROTOCOL TEST - {test_id}")
        print("=" * 80)
        print(f"{Colors.ENDC}\n")
        print(f"Testing endpoint: {Colors.OKBLUE}{self.endpoint}{Colors.ENDC}")
        print(f"Test: {test_description}")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # MANDATORY: Initialize resources using all-tables endpoint
        # Skip only for basic tests that don't need resources (tests 1-6)
        if test_id not in ['1', '1.1']:
            if not self._initialize_resources():
                print(f"\n{Colors.FAIL}✗ Initialization failed. Cannot proceed with test.{Colors.ENDC}")
                print(f"{Colors.WARNING}Please ensure the server is running and has at least one share with tables.{Colors.ENDC}")
                return
        
        # Run the specific test
        method = getattr(self, method_name)
        method()
        
        # Print detailed summary
        self._print_detailed_summary()
    
    def list_available_tests(self):
        """List all available tests"""
        print(f"\n{Colors.BOLD}Available Tests:{Colors.ENDC}\n")
        for test_id, (method_name, description) in sorted(self.available_tests.items(), key=lambda x: x[0]):
            print(f"  {Colors.OKCYAN}{test_id:4}{Colors.ENDC} - {description}")
        print()
    
    def _print_detailed_summary(self):
        """Print detailed test summary with individual test results"""
        self._print_separator("=")
        print(f"{Colors.BOLD}{Colors.HEADER}TEST SUMMARY{Colors.ENDC}\n")
        
        # Overall statistics
        total_duration = sum(r['duration'] for r in self.test_results)
        print(f"{Colors.BOLD}Overall Statistics:{Colors.ENDC}")
        print(f"  Total tests executed: {self.test_count}")
        print(f"  {Colors.OKGREEN}✓ Passed: {self.success_count}{Colors.ENDC}")
        print(f"  {Colors.WARNING}⚠ Warning: {len([r for r in self.test_results if r['status'] == 'WARNING'])}{Colors.ENDC}")
        print(f"  {Colors.FAIL}✗ Failed: {len([r for r in self.test_results if r['status'] == 'FAILED'])}{Colors.ENDC}")
        print(f"  Total duration: {total_duration:.3f} seconds")
        
        # Detailed results table
        print(f"\n{Colors.BOLD}Detailed Results:{Colors.ENDC}\n")
        print(f"  {'Test':<6} {'Status':<10} {'Status Code':<12} {'Duration':<12} {'Description'}")
        print(f"  {'-' * 6} {'-' * 10} {'-' * 12} {'-' * 12} {'-' * 40}")
        
        for result in self.test_results:
            test_num = result['number']
            test_name = result['name']
            status = result['status']
            duration_str = f"{result['duration']:.3f}s"
            
            # Color based on status
            if status == 'PASSED':
                status_display = f"{Colors.OKGREEN}✓ PASSED{Colors.ENDC}"
            elif status == 'WARNING':
                status_display = f"{Colors.WARNING}⚠ WARNING{Colors.ENDC}"
            else:
                status_display = f"{Colors.FAIL}✗ FAILED{Colors.ENDC}"
            
            # Status code or error
            if 'status_code' in result:
                status_code_display = str(result['status_code'])
            elif 'error' in result:
                status_code_display = 'N/A'
            else:
                status_code_display = 'N/A'
            
            print(f"  {test_num:<6} {status_display:<20} {status_code_display:<12} {duration_str:<12} {test_name[:40]}")
        
        # Failed tests details
        failed_tests = [r for r in self.test_results if r['status'] == 'FAILED']
        if failed_tests:
            print(f"\n{Colors.BOLD}{Colors.FAIL}Failed Tests Details:{Colors.ENDC}")
            for result in failed_tests:
                print(f"  Test {result['number']}: {result['name']}")
                if 'error' in result:
                    print(f"    Error: {result['error']}")
                print()
        
        # Completion time
        print(f"\n{Colors.BOLD}Completed at:{Colors.ENDC} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._print_separator("=")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Test Delta Sharing Protocol endpoints',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Run all tests (uses config.share by default)
  %(prog)s
  %(prog)s config.share
  
  # Run a specific test
  %(prog)s -t 1
  %(prog)s config.share -t 8.5
  
  # List available tests
  %(prog)s --list-tests
  %(prog)s config.share --list-tests
        '''
    )
    parser.add_argument(
        'config_file',
        nargs='?',
        default='config.share',
        help='Path to Delta Sharing configuration file (.share) - Default: config.share'
    )
    parser.add_argument(
        '-t', '--test',
        dest='test_id',
        metavar='TEST_ID',
        help='Run a specific test by ID (e.g., 1, 1.1, 8.5)'
    )
    parser.add_argument(
        '--list-tests',
        action='store_true',
        help='List all available tests and exit'
    )
    
    args = parser.parse_args()
    
    # Create tester instance
    tester = DeltaSharingTester(args.config_file)
    
    # List tests if requested
    if args.list_tests:
        tester.list_available_tests()
        return
    
    # Run specific test or all tests
    if args.test_id:
        tester.run_single_test(args.test_id)
    else:
        tester.run_all_tests()


if __name__ == '__main__':
    main()

