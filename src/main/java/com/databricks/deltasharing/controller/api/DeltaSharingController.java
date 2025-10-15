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
    
    // Server capabilities advertised to clients via Delta-Sharing-Capabilities header
    // Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#delta-sharing-capabilities-header
    // 
    // - responseformat=parquet: Server supports Parquet tables and Parquet response format
    //   Format: {"protocol": {...}}, {"metaData": {...}}, {"file": {...}}
    // 
    // - responseformat=delta: Server supports Delta tables and Delta response format
    //   Format: {"protocol": {"deltaProtocol": {...}}}, {"metaData": {"deltaMetadata": {...}}}, {"file": {"deltaSingleAction": {...}}}
    // 
    // Clients can specify their preferred format via query parameter: ?responseFormat=parquet or ?responseFormat=delta
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
    @GetMapping(value = "/shares/{share}/schemas/{schema}/tables/{table}/metadata", 
                produces = "application/x-ndjson")
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
            @PathVariable String table,
            @Parameter(description = "Delta Sharing Capabilities header")
            @RequestHeader(value = "delta-sharing-capabilities", required = false) String capabilitiesHeader,
            @Parameter(description = "Include End Stream Action header")
            @RequestHeader(value = "includeEndStreamAction", required = false) String includeEndStreamActionHeader) {
        
        log.info("Delta Sharing API: Querying metadata for table: {}.{}.{}, capabilities: {}, includeEndStreamAction: {}", 
                 share, schema, table, capabilitiesHeader, includeEndStreamActionHeader);
        
        QueryTableRequest request = new QueryTableRequest();
        parseCapabilitiesHeader(capabilitiesHeader, request);
        
        // Check for standalone includeEndStreamAction header
        if (includeEndStreamActionHeader != null && !includeEndStreamActionHeader.isEmpty()) {
            request.setIncludeEndStreamAction("true".equalsIgnoreCase(includeEndStreamActionHeader));
        }
        
        String response = deltaSharingService.queryTableMetadata(share, schema, table, request);
        
        // Build response headers
        ResponseEntity.BodyBuilder responseBuilder = ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .header("Delta-Table-Version", "1")
                .contentType(MediaType.parseMediaType("application/x-ndjson"));
        
        // Add includeEndStreamAction header if requested
        if (Boolean.TRUE.equals(request.getIncludeEndStreamAction())) {
            responseBuilder.header("includeEndStreamAction", "true");
        }
        
        return responseBuilder.body(response);
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
    @PostMapping(value = "/shares/{share}/schemas/{schema}/tables/{table}/query", 
                 produces = "application/x-ndjson")
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
            @Parameter(description = "Response format: 'parquet' or 'delta'")
            @RequestParam(required = false) String responseFormat,
            @Parameter(description = "Delta Sharing Capabilities header")
            @RequestHeader(value = "delta-sharing-capabilities", required = false) String capabilitiesHeader,
            @Parameter(description = "Include End Stream Action header")
            @RequestHeader(value = "includeEndStreamAction", required = false) String includeEndStreamActionHeader,
            @Parameter(description = "Query parameters (optional)")
            @RequestBody(required = false) QueryTableRequest request) {
        
        log.info("Delta Sharing API: Querying data for table: {}.{}.{}, responseFormat: {}, capabilities: {}, includeEndStreamAction: {}", 
                 share, schema, table, responseFormat, capabilitiesHeader, includeEndStreamActionHeader);
        
        if (request == null) {
            request = new QueryTableRequest();
        }
        
        // Parse capabilities header (includeEndStreamAction can be here)
        parseCapabilitiesHeader(capabilitiesHeader, request);
        
        // Check for standalone includeEndStreamAction header (Databricks client sends this)
        // This takes precedence over capabilities header
        if (includeEndStreamActionHeader != null && !includeEndStreamActionHeader.isEmpty()) {
            request.setIncludeEndStreamAction("true".equalsIgnoreCase(includeEndStreamActionHeader));
        }
        
        // Set responseFormat from query parameter if provided (takes precedence)
        if (responseFormat != null && !responseFormat.isEmpty()) {
            request.setResponseFormat(responseFormat);
        }
        
        String response = deltaSharingService.queryTableData(share, schema, table, request);
        
        // Build response headers
        ResponseEntity.BodyBuilder responseBuilder = ResponseEntity.ok()
                .header("Delta-Sharing-Capabilities", DELTA_SHARING_CAPABILITIES)
                .header("Delta-Table-Version", "1")
                .contentType(MediaType.parseMediaType("application/x-ndjson"));
        
        // Add includeEndStreamAction header if requested
        if (Boolean.TRUE.equals(request.getIncludeEndStreamAction())) {
            responseBuilder.header("includeEndStreamAction", "true");
        }
        
        return responseBuilder.body(response);
    }
    
    /**
     * Parse Delta Sharing Capabilities header
     * Format: "responseformat=delta;includeEndStreamAction=true;readerfeatures=deletionvectors,columnmapping"
     */
    private void parseCapabilitiesHeader(String capabilitiesHeader, QueryTableRequest request) {
        if (capabilitiesHeader == null || capabilitiesHeader.isEmpty()) {
            return;
        }
        
        // Split by semicolon
        String[] capabilities = capabilitiesHeader.split(";");
        for (String capability : capabilities) {
            String[] parts = capability.trim().split("=", 2);
            if (parts.length == 2) {
                String key = parts[0].trim().toLowerCase();
                String value = parts[1].trim().toLowerCase();
                
                switch (key) {
                    case "responseformat":
                        // Take first value if multiple provided
                        String format = value.split(",")[0].trim();
                        if (request.getResponseFormat() == null) {
                            request.setResponseFormat(format);
                        }
                        break;
                    case "includeendstreamaction":
                        request.setIncludeEndStreamAction("true".equals(value));
                        break;
                    // Future: handle readerfeatures
                }
            }
        }
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
