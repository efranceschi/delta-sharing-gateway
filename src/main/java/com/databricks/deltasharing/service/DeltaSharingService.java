package com.databricks.deltasharing.service;

import com.databricks.deltasharing.dto.protocol.*;
import com.databricks.deltasharing.exception.ResourceNotFoundException;
import com.databricks.deltasharing.model.DeltaSchema;
import com.databricks.deltasharing.model.DeltaShare;
import com.databricks.deltasharing.model.DeltaTable;
import com.databricks.deltasharing.repository.DeltaSchemaRepository;
import com.databricks.deltasharing.repository.DeltaShareRepository;
import com.databricks.deltasharing.repository.DeltaTableRepository;
import com.databricks.deltasharing.service.storage.FileStorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service implementing Delta Sharing Protocol operations
 * Based on Delta Sharing Protocol specification
 */
@Service
@Slf4j
public class DeltaSharingService {
    
    private final DeltaShareRepository shareRepository;
    private final DeltaSchemaRepository schemaRepository;
    private final DeltaTableRepository tableRepository;
    private final FileStorageService fileStorageService;
    private final PaginationService paginationService;
    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper;
    private final com.fasterxml.jackson.databind.ObjectMapper ndjsonObjectMapper;
    
    /**
     * Constructor with dependency injection
     * Uses @Qualifier to inject the correct ObjectMapper for NDJSON responses
     */
    public DeltaSharingService(
            DeltaShareRepository shareRepository,
            DeltaSchemaRepository schemaRepository,
            DeltaTableRepository tableRepository,
            FileStorageService fileStorageService,
            PaginationService paginationService,
            com.fasterxml.jackson.databind.ObjectMapper objectMapper,
            @Qualifier("ndjsonObjectMapper") com.fasterxml.jackson.databind.ObjectMapper ndjsonObjectMapper) {
        this.shareRepository = shareRepository;
        this.schemaRepository = schemaRepository;
        this.tableRepository = tableRepository;
        this.fileStorageService = fileStorageService;
        this.paginationService = paginationService;
        this.objectMapper = objectMapper;
        this.ndjsonObjectMapper = ndjsonObjectMapper;
    }
    
    /**
     * List all shares
     * Endpoint: GET /shares
     */
    @Transactional(readOnly = true)
    public ListSharesResponse listShares(Integer maxResults, String pageToken) {
        log.debug("Listing shares with maxResults: {}, pageToken: {}", maxResults, pageToken);
        
        List<DeltaShare> shares = shareRepository.findByActiveTrue();
        
        List<ShareResponse> allItems = shares.stream()
                .map(this::convertToShareResponse)
                .collect(Collectors.toList());
        
        // Apply pagination
        PaginationService.PaginatedResult<ShareResponse> paginatedResult = 
                paginationService.paginate(allItems, maxResults, pageToken);
        
        return ListSharesResponse.builder()
                .items(paginatedResult.getItems())
                .nextPageToken(paginatedResult.getNextPageToken())
                .build();
    }
    
    /**
     * Get a specific share
     * Endpoint: GET /shares/{share}
     * 
     * Returns share information wrapped in "share" field per protocol specification:
     * {"share": {"name": "...", "id": "..."}}
     */
    @Transactional(readOnly = true)
    public GetShareResponse getShare(String shareName) {
        log.debug("Getting share: {}", shareName);
        
        verifyShareIsActive(shareName);
        
        DeltaShare share = shareRepository.findByName(shareName)
                .orElseThrow(() -> new ResourceNotFoundException("Share not found: " + shareName));
        
        ShareResponse shareResponse = convertToShareResponse(share);
        
        // Wrap in GetShareResponse to add the "share" wrapper per protocol
        return GetShareResponse.builder()
                .share(shareResponse)
                .build();
    }
    
    /**
     * List all schemas in a share
     * Endpoint: GET /shares/{share}/schemas
     */
    @Transactional(readOnly = true)
    public ListSchemasResponse listSchemas(String shareName, Integer maxResults, String pageToken) {
        log.debug("Listing schemas for share: {}", shareName);
        
        verifyShareIsActive(shareName);
        
        List<DeltaSchema> schemas = schemaRepository.findByShareName(shareName);
        
        List<SchemaResponse> allItems = schemas.stream()
                .map(schema -> convertToSchemaResponse(schema, shareName))
                .collect(Collectors.toList());
        
        // Apply pagination
        PaginationService.PaginatedResult<SchemaResponse> paginatedResult = 
                paginationService.paginate(allItems, maxResults, pageToken);
        
        return ListSchemasResponse.builder()
                .items(paginatedResult.getItems())
                .nextPageToken(paginatedResult.getNextPageToken())
                .build();
    }
    
    /**
     * List all tables in a schema
     * Endpoint: GET /shares/{share}/schemas/{schema}/tables
     */
    @Transactional(readOnly = true)
    public ListTablesResponse listTables(String shareName, String schemaName, 
                                          Integer maxResults, String pageToken) {
        log.debug("Listing tables for share: {}, schema: {}", shareName, schemaName);
        
        verifyShareIsActive(shareName);
        
        // Verify schema exists
        schemaRepository.findByNameAndShareName(schemaName, shareName)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Schema not found: " + schemaName + " in share: " + shareName));
        
        List<DeltaTable> tables = tableRepository.findBySchemaNameAndShareName(schemaName, shareName);
        
        List<TableResponse> allItems = tables.stream()
                .map(table -> convertToTableResponse(table, schemaName, shareName))
                .collect(Collectors.toList());
        
        // Apply pagination
        PaginationService.PaginatedResult<TableResponse> paginatedResult = 
                paginationService.paginate(allItems, maxResults, pageToken);
        
        return ListTablesResponse.builder()
                .items(paginatedResult.getItems())
                .nextPageToken(paginatedResult.getNextPageToken())
                .build();
    }
    
    /**
     * List all tables in all schemas of a share
     * Endpoint: GET /shares/{share}/all-tables
     */
    @Transactional(readOnly = true)
    public ListTablesResponse listAllTables(String shareName, Integer maxResults, String pageToken) {
        log.debug("Listing all tables for share: {}", shareName);
        
        verifyShareIsActive(shareName);
        
        List<DeltaTable> tables = tableRepository.findByShareName(shareName);
        
        List<TableResponse> allItems = tables.stream()
                .map(table -> convertToTableResponse(
                        table, 
                        table.getSchema().getName(), 
                        shareName))
                .collect(Collectors.toList());
        
        // Apply pagination
        PaginationService.PaginatedResult<TableResponse> paginatedResult = 
                paginationService.paginate(allItems, maxResults, pageToken);
        
        return ListTablesResponse.builder()
                .items(paginatedResult.getItems())
                .nextPageToken(paginatedResult.getNextPageToken())
                .build();
    }
    
    /**
     * Query table metadata
     * Endpoint: GET /shares/{share}/schemas/{schema}/tables/{table}/metadata
     */
    @Transactional(readOnly = true)
    public String queryTableMetadata(String shareName, String schemaName, String tableName, QueryTableRequest request) {
        log.debug("Querying metadata for table: {}.{}.{}", shareName, schemaName, tableName);
        
        verifyShareIsActive(shareName);
        
        DeltaTable table = tableRepository.findByNameAndSchemaNameAndShareName(
                tableName, schemaName, shareName)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Table not found: " + tableName + " in schema: " + schemaName + 
                        " in share: " + shareName));
        
        // Return newline-delimited JSON (NDJSON) format
        StringBuilder response = new StringBuilder();
        
        // Determine response format based on table type and client request
        boolean isDeltaTable = "delta".equalsIgnoreCase(table.getFormat());
        String requestedFormat = request != null ? request.getResponseFormat() : null;
        
        // According to Delta Sharing Protocol:
        // - If client sends a list (e.g., "parquet,delta"), they support both formats
        // - Server MUST respond in delta format for Delta tables (tables with advanced features)
        // - Server MAY respond in parquet format for Parquet tables (simple tables)
        boolean clientSupportsDelta = requestedFormat != null && 
                                      (requestedFormat.contains("delta") || "delta".equalsIgnoreCase(requestedFormat));
        
        // Use delta format if:
        // 1. Table is Delta AND client supports delta format (explicitly or in list), OR
        // 2. Table is Delta AND client doesn't specify format (default to delta for delta tables)
        boolean useDeltaFormat = isDeltaTable || 
                                 (clientSupportsDelta || requestedFormat == null || requestedFormat.isEmpty());
        
        log.info("📊 Metadata Format Decision - Table: {}, Format: {}, Requested: {}, ClientSupportsDelta: {}, UseDeltaFormat: {}", 
                  tableName, table.getFormat(), requestedFormat, clientSupportsDelta, useDeltaFormat);
        
        // Protocol line
        // For Delta format: include both minReaderVersion and minWriterVersion
        // For Parquet format: include only minReaderVersion
        // Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
        // Note: Delta Sharing Protocol versions are different from Delta Lake Protocol versions
        // Delta Sharing uses minReaderVersion=1 and minWriterVersion=2
        ProtocolResponse.ProtocolResponseBuilder protocolBuilder = ProtocolResponse.builder()
                .minReaderVersion(1);
        
        if (useDeltaFormat) {
            // Delta format requires minWriterVersion
            // Using minWriterVersion=2 for Delta Sharing Protocol
            protocolBuilder.minWriterVersion(2);
        }
        
        ProtocolResponse protocol = protocolBuilder.build();
        String protocolJson;
        if (useDeltaFormat) {
            // Delta format: {"protocol": {"deltaProtocol": {...}}}
            DeltaProtocolWrapper wrapper = DeltaProtocolWrapper.builder()
                    .deltaProtocol(protocol)
                    .build();
            protocolJson = String.format("{\"protocol\":%s}", toNdjson(wrapper));
        } else {
            // Parquet format: {"protocol": {...}}
            protocolJson = String.format("{\"protocol\":%s}", toNdjson(protocol));
        }
        response.append(protocolJson).append("\n");
        
        // Metadata line - Always use simple format for /metadata endpoint
        // Generate dynamic schema based on table name and format (delegated to storage service)
        String schemaString = fileStorageService.getTableSchema(table.getName(), table.getFormat());
        
        // Get partition columns (delegated to storage service)
        String[] partCols = fileStorageService.getPartitionColumns(table.getName());
        List<String> partitionColumns = Arrays.asList(partCols);
        
        // Calculate table size and number of files
        List<FileResponse> files = fileStorageService.getTableFiles(table, null, null, null);
        long totalSize = files.stream()
                .filter(f -> f.getSize() != null)
                .mapToLong(FileResponse::getSize)
                .sum();
        long numFiles = files.size();
        
        MetadataResponse metadata = MetadataResponse.builder()
                .id(table.getUuid())
                .format(FormatResponse.builder()
                        .provider(table.getFormat())
                        .build())
                .schemaString(schemaString)
                .partitionColumns(partitionColumns)
                .configuration(new HashMap<>())
                .size(totalSize)
                .numFiles(numFiles)
                .build();
        String metadataJson = String.format("{\"metaData\":%s}", toNdjson(metadata));
        response.append(metadataJson).append("\n");
        
        // Add EndStreamAction if requested by client
        if (request != null && Boolean.TRUE.equals(request.getIncludeEndStreamAction())) {
            EndStreamAction action = EndStreamAction.builder()
                    .refreshToken(generateRefreshToken(shareName, schemaName, tableName))
                    .build();
            
            // Wrap in EndStreamActionWrapper to match protocol format
            EndStreamActionWrapper wrapper = EndStreamActionWrapper.builder()
                    .endStreamAction(action)
                    .build();
            
            String endStreamJson = toNdjson(wrapper);
            response.append(endStreamJson).append("\n");
            
            log.debug("Added EndStreamAction to metadata response");
        }
        
        // Debug: Log the full response for troubleshooting
        String fullResponse = response.toString();
        log.debug("Metadata response for table {}: {} lines, {} chars", 
                tableName, fullResponse.split("\n").length, fullResponse.length());
        if (log.isTraceEnabled()) {
            log.trace("Full metadata response:\n{}", fullResponse);
        }
        
        return fullResponse;
    }
    
    /**
     * Query table version
     * Endpoint: GET /shares/{share}/schemas/{schema}/tables/{table}/version
     */
    @Transactional(readOnly = true)
    public Long queryTableVersion(String shareName, String schemaName, String tableName) {
        log.debug("Querying version for table: {}.{}.{}", shareName, schemaName, tableName);
        
        verifyShareIsActive(shareName);
        
        DeltaTable table = tableRepository.findByNameAndSchemaNameAndShareName(
                tableName, schemaName, shareName)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Table not found: " + tableName + " in schema: " + schemaName + 
                        " in share: " + shareName));
        
        // Get current version from file storage service
        if (fileStorageService instanceof com.databricks.deltasharing.service.storage.MinIOFileStorageService) {
            com.databricks.deltasharing.service.storage.MinIOFileStorageService minioService = 
                (com.databricks.deltasharing.service.storage.MinIOFileStorageService) fileStorageService;
            return minioService.getTableVersion(table.getName());
        }
        
        // Default to version 0 for other storage services
        return 0L;
    }
    
    /**
     * Query table data (files)
     * Endpoint: POST /shares/{share}/schemas/{schema}/tables/{table}/query
     */
    @Transactional(readOnly = true)
    public String queryTableData(String shareName, String schemaName, String tableName, 
                                  QueryTableRequest request) {
        log.debug("Querying data for table: {}.{}.{} with storage type: {}", 
                  shareName, schemaName, tableName, fileStorageService.getStorageType());
        
        verifyShareIsActive(shareName);
        
        DeltaTable table = tableRepository.findByNameAndSchemaNameAndShareName(
                tableName, schemaName, shareName)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Table not found: " + tableName + " in schema: " + schemaName + 
                        " in share: " + shareName));
        
        // Return newline-delimited JSON (NDJSON) format
        StringBuilder response = new StringBuilder();
        
        // Determine response format based on table type and client request
        // Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#delta-sharing-capabilities-header
        boolean isDeltaTable = "delta".equalsIgnoreCase(table.getFormat());
        String requestedFormat = request.getResponseFormat();
        
        // According to Delta Sharing Protocol:
        // - If client sends a list (e.g., "parquet,delta"), they support both formats
        // - Server MUST respond in delta format for Delta tables (tables with advanced features)
        // - Server MAY respond in parquet format for Parquet tables (simple tables)
        boolean clientSupportsDelta = requestedFormat != null && 
                                      (requestedFormat.contains("delta") || "delta".equalsIgnoreCase(requestedFormat));
        
        // Use delta format if:
        // 1. Table is Delta AND client supports delta format (explicitly or in list), OR
        // 2. Table is Delta AND client doesn't specify format (default to delta for delta tables)
        boolean useDeltaFormat = isDeltaTable || 
                                 (clientSupportsDelta || requestedFormat == null || requestedFormat.isEmpty());
        
        log.info("📊 Format Decision - Table: {}, Format: {}, Requested: {}, ClientSupportsDelta: {}, UseDeltaFormat: {}", 
                  tableName, table.getFormat(), requestedFormat, clientSupportsDelta, useDeltaFormat);
        
        // Protocol line
        // For Delta format: include both minReaderVersion and minWriterVersion
        // For Parquet format: include only minReaderVersion
        // Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
        // Note: Delta Sharing Protocol versions are different from Delta Lake Protocol versions
        // Delta Sharing uses minReaderVersion=1 and minWriterVersion=2
        ProtocolResponse.ProtocolResponseBuilder protocolBuilder = ProtocolResponse.builder()
                .minReaderVersion(1);
        
        if (useDeltaFormat) {
            // Delta format requires minWriterVersion
            // Using minWriterVersion=2 for Delta Sharing Protocol
            protocolBuilder.minWriterVersion(2);
        }
        
        ProtocolResponse protocol = protocolBuilder.build();
        String protocolJson;
        if (useDeltaFormat) {
            // Delta format: {"protocol": {"deltaProtocol": {...}}}
            DeltaProtocolWrapper wrapper = DeltaProtocolWrapper.builder()
                    .deltaProtocol(protocol)
                    .build();
            protocolJson = String.format("{\"protocol\":%s}", toNdjson(wrapper));
        } else {
            // Parquet format: {"protocol": {...}}
            protocolJson = String.format("{\"protocol\":%s}", toNdjson(protocol));
        }
        response.append(protocolJson).append("\n");
        
        // Metadata line
        // Generate dynamic schema based on table name and format (delegated to storage service)
        String schemaString = fileStorageService.getTableSchema(table.getName(), table.getFormat());
        
        // Get partition columns (delegated to storage service)
        String[] partCols = fileStorageService.getPartitionColumns(table.getName());
        List<String> partitionColumns = Arrays.asList(partCols);
        
        // Metadata line
        MetadataResponse metadata = MetadataResponse.builder()
                .id(table.getUuid())
                .name(table.getName())
                .format(FormatResponse.builder()
                        .provider(table.getFormat())
                        .build())
                .schemaString(schemaString)
                .partitionColumns(partitionColumns)
                .build();
        String metadataJson;
        if (useDeltaFormat) {
            // Delta format: {"metaData": {"deltaMetadata": {...}}}
            DeltaMetadataWrapper wrapper = DeltaMetadataWrapper.builder()
                    .deltaMetadata(metadata)
                    .build();
            metadataJson = String.format("{\"metaData\":%s}", toNdjson(wrapper));
        } else {
            // Parquet format: {"metaData": {...}}
            metadataJson = String.format("{\"metaData\":%s}", toNdjson(metadata));
        }
        response.append(metadataJson).append("\n");
        
        // Get files from storage service
        List<FileResponse> files = fileStorageService.getTableFiles(
                table, 
                request.getVersion(), 
                request.getPredicateHints(), 
                request.getLimitHint()
        );
        
        // Add file lines to response
        Long minExpirationTimestamp = null;
        for (FileResponse file : files) {
            String fileJson;
            if (useDeltaFormat) {
                // Delta format: {"file": {"deltaSingleAction": {...}}}
                DeltaSingleActionWrapper wrapper = DeltaSingleActionWrapper.builder()
                        .deltaSingleAction(file)
                        .build();
                fileJson = String.format("{\"file\":%s}", toNdjson(wrapper));
            } else {
                // Parquet format: {"file": {...}}
                fileJson = String.format("{\"file\":%s}", toNdjson(file));
            }
            response.append(fileJson).append("\n");
            
            // Track minimum expiration timestamp across all files
            if (file.getExpirationTimestamp() != null) {
                if (minExpirationTimestamp == null || file.getExpirationTimestamp() < minExpirationTimestamp) {
                    minExpirationTimestamp = file.getExpirationTimestamp();
                }
            }
        }
        
        // Add EndStreamAction if requested by client
        // Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#endstreamaction
        // When includeEndStreamAction=true, the server MUST include EndStreamAction in the response
        // The client will throw an exception if it's missing
        if (Boolean.TRUE.equals(request.getIncludeEndStreamAction())) {
            EndStreamAction action = EndStreamAction.builder()
                    .refreshToken(generateRefreshToken(shareName, schemaName, tableName))
                    .minUrlExpirationTimestamp(minExpirationTimestamp)
                    .build();
            
            // Wrap in EndStreamActionWrapper to match protocol format
            EndStreamActionWrapper wrapper = EndStreamActionWrapper.builder()
                    .endStreamAction(action)
                    .build();
            
            String endStreamJson = toNdjson(wrapper);
            response.append(endStreamJson).append("\n");
            
            log.debug("Added EndStreamAction to response with minUrlExpirationTimestamp: {} (REQUIRED by client)", 
                     minExpirationTimestamp);
        } else {
            log.debug("EndStreamAction not requested by client, skipping");
        }
        
        log.info("Returning {} files for table: {}.{}.{}", files.size(), shareName, schemaName, tableName);
        
        return response.toString();
    }
    
    /**
     * Generate a refresh token for the client to refresh pre-signed URLs
     * The token should encode the table information and be verifiable by the server
     */
    private String generateRefreshToken(String shareName, String schemaName, String tableName) {
        // Create a simple token encoding the table path
        // In production, this should be encrypted/signed for security
        String tokenData = String.format("%s:%s:%s:%d", shareName, schemaName, tableName, System.currentTimeMillis());
        return java.util.Base64.getEncoder().encodeToString(tokenData.getBytes());
    }
    
    /**
     * Query table changes (CDF - Change Data Feed)
     * Endpoint: GET /shares/{share}/schemas/{schema}/tables/{table}/changes
     */
    @Transactional(readOnly = true)
    public String queryTableChanges(String shareName, String schemaName, String tableName,
                                     Long startingVersion, Long endingVersion) {
        log.debug("Querying changes for table: {}.{}.{} from version {} to {}", 
                  shareName, schemaName, tableName, startingVersion, endingVersion);
        
        verifyShareIsActive(shareName);
        
        DeltaTable table = tableRepository.findByNameAndSchemaNameAndShareName(
                tableName, schemaName, shareName)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Table not found: " + tableName + " in schema: " + schemaName + 
                        " in share: " + shareName));
        
        // Check if storage service supports CDF
        if (!(fileStorageService instanceof com.databricks.deltasharing.service.storage.MinIOFileStorageService)) {
            log.warn("CDF not supported for storage type: {}", fileStorageService.getStorageType());
            return "";
        }
        
        com.databricks.deltasharing.service.storage.MinIOFileStorageService minioService = 
            (com.databricks.deltasharing.service.storage.MinIOFileStorageService) fileStorageService;
        
        // Get changes from storage service
        List<FileResponse> changes = minioService.getTableChanges(table, startingVersion, endingVersion);
        
        // Return newline-delimited JSON (NDJSON) format
        StringBuilder response = new StringBuilder();
        
        // Protocol line - Per Delta Sharing Protocol specification
        ProtocolResponse protocol = ProtocolResponse.builder()
                .minReaderVersion(1)
                .build();
        String protocolJson = String.format("{\"protocol\":%s}", toNdjson(protocol));
        response.append(protocolJson).append("\n");
        
        // Metadata line
        String schemaString = fileStorageService.getTableSchema(table.getName(), table.getFormat());
        String[] partCols = fileStorageService.getPartitionColumns(table.getName());
        List<String> partitionColumns = Arrays.asList(partCols);
        
        MetadataResponse metadata = MetadataResponse.builder()
                .id(table.getUuid())
                .name(table.getName())
                .format(FormatResponse.builder()
                        .provider(table.getFormat())
                        .build())
                .schemaString(schemaString)
                .partitionColumns(partitionColumns)
                .build();
        String metadataJson = String.format("{\"metaData\":%s}", toNdjson(metadata));
        response.append(metadataJson).append("\n");
        
        // Add change entries
        for (FileResponse change : changes) {
            String changeJson = String.format("{\"file\":%s}", toNdjson(change));
            response.append(changeJson).append("\n");
        }
        
        log.info("Returning {} changes for table: {}.{}.{}", changes.size(), shareName, schemaName, tableName);
        
        return response.toString();
    }
    
    // Helper methods for conversion and validation
    
    /**
     * Verify if a share exists and is active
     * @param shareName the share name to verify
     * @throws ResourceNotFoundException if share not found or not active
     */
    private void verifyShareIsActive(String shareName) {
        DeltaShare share = shareRepository.findByName(shareName)
                .orElseThrow(() -> new ResourceNotFoundException("Share not found: " + shareName));
        
        if (share.getActive() == null || !share.getActive()) {
            log.warn("Attempted to access inactive share: {}", shareName);
            throw new ResourceNotFoundException("Share not found: " + shareName);
        }
    }
    
    private ShareResponse convertToShareResponse(DeltaShare share) {
        return ShareResponse.builder()
                .name(share.getName())
                .id(share.getUuid()) // Use UUID instead of numeric ID for Databricks compatibility
                .build();
    }
    
    private SchemaResponse convertToSchemaResponse(DeltaSchema schema, String shareName) {
        return SchemaResponse.builder()
                .name(schema.getName())
                .share(shareName)
                .build();
    }
    
    private TableResponse convertToTableResponse(DeltaTable table, String schemaName, String shareName) {
        // Get share UUID for Databricks compatibility
        String shareUuid = null;
        if (table.getSchema() != null && table.getSchema().getShare() != null) {
            shareUuid = table.getSchema().getShare().getUuid();
        }
        
        return TableResponse.builder()
                .name(table.getName())
                .schema(schemaName)
                .share(shareName)
                .shareAsView(table.getShareAsView())
                .id(table.getUuid()) // Use UUID instead of numeric ID for Databricks compatibility
                .shareId(shareUuid) // Share UUID for Databricks compatibility
                .build();
    }
    
    /**
     * Convert object to JSON string with pretty printing (2 spaces indentation)
     * Reserved for future use in regular JSON responses (GET /shares, GET /schemas, etc.)
     * 
     * Note: Currently, regular JSON endpoints (GET /shares, etc.) use Spring's automatic
     * serialization via @RestController, which uses the @Primary ObjectMapper bean.
     * This method is kept for potential future manual JSON serialization needs.
     * 
     * Uses configured ObjectMapper with INDENT_OUTPUT enabled
     */
    @SuppressWarnings("unused")
    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Error converting to JSON: {}", e.getMessage(), e);
            return "{}";
        }
    }
    
    /**
     * Convert object to compact JSON string (NO pretty printing)
     * Used for NDJSON responses (POST /query, GET /metadata, GET /changes)
     * 
     * NDJSON (Newline-Delimited JSON) requires each line to be a complete, compact JSON object
     * Example: {"protocol":{"minReaderVersion":1}}
     */
    private String toNdjson(Object obj) {
        try {
            return ndjsonObjectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Error converting to NDJSON: {}", e.getMessage(), e);
            return "{}";
        }
    }
}
