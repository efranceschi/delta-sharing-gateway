package com.databricks.deltasharing.controller.api;

import com.databricks.deltasharing.dto.protocol.*;
import com.databricks.deltasharing.service.DeltaSharingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Delta Sharing Protocol REST API Controller
 * Implements the Delta Sharing Protocol specification
 * 
 * Based on: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
 */
@RestController
@RequestMapping("/delta-sharing")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Delta Sharing Protocol", description = "Delta Sharing Protocol REST API endpoints")
@SecurityRequirement(name = "bearerAuth")
public class DeltaSharingController {
    
    private final DeltaSharingService deltaSharingService;
    
    // Server supports both response formats:
    // - responseformat=parquet: Simple format for Parquet tables {"protocol": {...}}
    // - responseformat=delta: Wrapped format for Delta tables {"protocol": {"deltaProtocol": {...}}}
    // Format is automatically selected based on table.format field
    private static final String DELTA_SHARING_CAPABILITIES = "responseformat=parquet,delta";
    
    /**
     * List all shares
     * GET /shares
     */
    @GetMapping("/shares")
    @Operation(summary = "List Shares", 
               description = "List all shares available to the authenticated user")
    @ApiResponse(responseCode = "200", description = "Successfully retrieved shares")
    public ResponseEntity<ListSharesResponse> listShares(
            @Parameter(description = "Maximum number of results to return")
            @RequestParam(required = false) Integer maxResults,
            @Parameter(description = "Page token for pagination")
            @RequestParam(required = false) String pageToken) {
        
        log.info("Delta Sharing API: Listing shares");
        ListSharesResponse response = deltaSharingService.listShares(maxResults, pageToken);
        
        return ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .body(response);
    }
    
    /**
     * Get a specific share
     * GET /shares/{share}
     * 
     * Returns: {"share": {"name": "...", "id": "..."}}
     */
    @GetMapping("/shares/{share}")
    @Operation(summary = "Get Share", 
               description = "Get information about a specific share")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved share"),
        @ApiResponse(responseCode = "404", description = "Share not found")
    })
    public ResponseEntity<GetShareResponse> getShare(
            @Parameter(description = "Share name", required = true)
            @PathVariable String share) {
        
        log.info("Delta Sharing API: Getting share: {}", share);
        GetShareResponse response = deltaSharingService.getShare(share);
        
        return ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .body(response);
    }
    
    /**
     * List all schemas in a share
     * GET /shares/{share}/schemas
     */
    @GetMapping("/shares/{share}/schemas")
    @Operation(summary = "List Schemas", 
               description = "List all schemas in a share")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved schemas"),
        @ApiResponse(responseCode = "404", description = "Share not found")
    })
    public ResponseEntity<ListSchemasResponse> listSchemas(
            @Parameter(description = "Share name", required = true)
            @PathVariable String share,
            @Parameter(description = "Maximum number of results to return")
            @RequestParam(required = false) Integer maxResults,
            @Parameter(description = "Page token for pagination")
            @RequestParam(required = false) String pageToken) {
        
        log.info("Delta Sharing API: Listing schemas for share: {}", share);
        ListSchemasResponse response = deltaSharingService.listSchemas(share, maxResults, pageToken);
        
        return ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .body(response);
    }
    
    /**
     * List all tables in a schema
     * GET /shares/{share}/schemas/{schema}/tables
     */
    @GetMapping("/shares/{share}/schemas/{schema}/tables")
    @Operation(summary = "List Tables", 
               description = "List all tables in a schema")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved tables"),
        @ApiResponse(responseCode = "404", description = "Share or schema not found")
    })
    public ResponseEntity<ListTablesResponse> listTables(
            @Parameter(description = "Share name", required = true)
            @PathVariable String share,
            @Parameter(description = "Schema name", required = true)
            @PathVariable String schema,
            @Parameter(description = "Maximum number of results to return")
            @RequestParam(required = false) Integer maxResults,
            @Parameter(description = "Page token for pagination")
            @RequestParam(required = false) String pageToken) {
        
        log.info("Delta Sharing API: Listing tables for share: {}, schema: {}", share, schema);
        ListTablesResponse response = deltaSharingService.listTables(
                share, schema, maxResults, pageToken);
        
        return ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .body(response);
    }
    
    /**
     * List all tables in all schemas of a share
     * GET /shares/{share}/all-tables
     */
    @GetMapping("/shares/{share}/all-tables")
    @Operation(summary = "List All Tables", 
               description = "List all tables across all schemas in a share")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved tables"),
        @ApiResponse(responseCode = "404", description = "Share not found")
    })
    public ResponseEntity<ListTablesResponse> listAllTables(
            @Parameter(description = "Share name", required = true)
            @PathVariable String share,
            @Parameter(description = "Maximum number of results to return")
            @RequestParam(required = false) Integer maxResults,
            @Parameter(description = "Page token for pagination")
            @RequestParam(required = false) String pageToken) {
        
        log.info("Delta Sharing API: Listing all tables for share: {}", share);
        ListTablesResponse response = deltaSharingService.listAllTables(share, maxResults, pageToken);
        
        return ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .body(response);
    }
    
    /**
     * Query table metadata
     * GET /shares/{share}/schemas/{schema}/tables/{table}/metadata
     * 
     * Returns newline-delimited JSON (NDJSON) with protocol and metadata
     */
    @GetMapping("/shares/{share}/schemas/{schema}/tables/{table}/metadata")
    @Operation(summary = "Query Table Metadata", 
               description = "Get metadata for a specific table (returns NDJSON format)")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved metadata"),
        @ApiResponse(responseCode = "404", description = "Share, schema, or table not found")
    })
    public ResponseEntity<String> queryTableMetadata(
            @Parameter(description = "Share name", required = true)
            @PathVariable String share,
            @Parameter(description = "Schema name", required = true)
            @PathVariable String schema,
            @Parameter(description = "Table name", required = true)
            @PathVariable String table) {
        
        log.info("Delta Sharing API: Querying metadata for table: {}.{}.{}", share, schema, table);
        String response = deltaSharingService.queryTableMetadata(share, schema, table);
        
        return ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .header("Delta-Table-Version", "1")
                .contentType(MediaType.parseMediaType("application/x-ndjson"))
                .body(response);
    }
    
    /**
     * Query table version
     * GET /shares/{share}/schemas/{schema}/tables/{table}/version
     * 
     * Returns the current version of the table
     */
    @GetMapping("/shares/{share}/schemas/{schema}/tables/{table}/version")
    @Operation(summary = "Query Table Version", 
               description = "Get the current version of a table")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved version"),
        @ApiResponse(responseCode = "404", description = "Share, schema, or table not found")
    })
    public ResponseEntity<String> queryTableVersion(
            @Parameter(description = "Share name", required = true)
            @PathVariable String share,
            @Parameter(description = "Schema name", required = true)
            @PathVariable String schema,
            @Parameter(description = "Table name", required = true)
            @PathVariable String table) {
        
        log.info("Delta Sharing API: Querying version for table: {}.{}.{}", share, schema, table);
        
        // Get actual table version from service
        Long version = deltaSharingService.queryTableVersion(share, schema, table);
        String response = String.format("{\"deltaTableVersion\":%d}", version);
        
        return ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .contentType(MediaType.APPLICATION_JSON)
                .body(response);
    }
    
    /**
     * Query table data (files)
     * POST /shares/{share}/schemas/{schema}/tables/{table}/query
     * 
     * Returns newline-delimited JSON (NDJSON) with protocol, metadata, and file references
     */
    @PostMapping("/shares/{share}/schemas/{schema}/tables/{table}/query")
    @Operation(summary = "Query Table Data", 
               description = "Query table data files (returns NDJSON format with file references)")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved data"),
        @ApiResponse(responseCode = "404", description = "Share, schema, or table not found")
    })
    public ResponseEntity<String> queryTableData(
            @Parameter(description = "Share name", required = true)
            @PathVariable String share,
            @Parameter(description = "Schema name", required = true)
            @PathVariable String schema,
            @Parameter(description = "Table name", required = true)
            @PathVariable String table,
            @Parameter(description = "Query parameters (optional)")
            @RequestBody(required = false) QueryTableRequest request) {
        
        log.info("Delta Sharing API: Querying data for table: {}.{}.{}", share, schema, table);
        
        if (request == null) {
            request = new QueryTableRequest();
        }
        
        String response = deltaSharingService.queryTableData(share, schema, table, request);
        
        return ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .header("Delta-Table-Version", "1")
                .contentType(MediaType.parseMediaType("application/x-ndjson"))
                .body(response);
    }
    
    /**
     * Query table changes (Change Data Feed)
     * GET /shares/{share}/schemas/{schema}/tables/{table}/changes
     * 
     * Returns table changes for CDC (Change Data Capture)
     */
    @GetMapping("/shares/{share}/schemas/{schema}/tables/{table}/changes")
    @Operation(summary = "Query Table Changes", 
               description = "Query table changes for Change Data Feed (CDC)")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved changes"),
        @ApiResponse(responseCode = "404", description = "Share, schema, or table not found")
    })
    public ResponseEntity<String> queryTableChanges(
            @Parameter(description = "Share name", required = true)
            @PathVariable String share,
            @Parameter(description = "Schema name", required = true)
            @PathVariable String schema,
            @Parameter(description = "Table name", required = true)
            @PathVariable String table,
            @Parameter(description = "Starting version for CDF")
            @RequestParam(required = false) Long startingVersion,
            @Parameter(description = "Ending version for CDF")
            @RequestParam(required = false) Long endingVersion) {
        
        log.info("Delta Sharing API: Querying changes for table: {}.{}.{} from version {} to {}", 
                share, schema, table, startingVersion, endingVersion);
        
        // Get table changes from service
        String response = deltaSharingService.queryTableChanges(
                share, schema, table, startingVersion, endingVersion);
        
        return ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .header("Delta-Table-Version", "1")
                .contentType(MediaType.parseMediaType("application/x-ndjson"))
                .body(response);
    }
}
