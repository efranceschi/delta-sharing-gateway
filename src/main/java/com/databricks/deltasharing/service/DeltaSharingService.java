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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service implementing Delta Sharing Protocol operations
 * Based on Delta Sharing Protocol specification
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DeltaSharingService {
    
    private final DeltaShareRepository shareRepository;
    private final DeltaSchemaRepository schemaRepository;
    private final DeltaTableRepository tableRepository;
    private final FileStorageService fileStorageService;
    
    /**
     * List all shares
     * Endpoint: GET /shares
     */
    @Transactional(readOnly = true)
    public ListSharesResponse listShares(Integer maxResults, String pageToken) {
        log.debug("Listing shares with maxResults: {}, pageToken: {}", maxResults, pageToken);
        
        List<DeltaShare> shares = shareRepository.findByActiveTrue();
        
        List<ShareResponse> items = shares.stream()
                .map(this::convertToShareResponse)
                .collect(Collectors.toList());
        
        return ListSharesResponse.builder()
                .items(items)
                .nextPageToken(null) // Pagination not implemented yet
                .build();
    }
    
    /**
     * Get a specific share
     * Endpoint: GET /shares/{share}
     */
    @Transactional(readOnly = true)
    public ShareResponse getShare(String shareName) {
        log.debug("Getting share: {}", shareName);
        
        verifyShareIsActive(shareName);
        
        DeltaShare share = shareRepository.findByName(shareName)
                .orElseThrow(() -> new ResourceNotFoundException("Share not found: " + shareName));
        
        return convertToShareResponse(share);
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
        
        List<SchemaResponse> items = schemas.stream()
                .map(schema -> convertToSchemaResponse(schema, shareName))
                .collect(Collectors.toList());
        
        return ListSchemasResponse.builder()
                .items(items)
                .nextPageToken(null)
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
        
        List<TableResponse> items = tables.stream()
                .map(table -> convertToTableResponse(table, schemaName, shareName))
                .collect(Collectors.toList());
        
        return ListTablesResponse.builder()
                .items(items)
                .nextPageToken(null)
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
        
        List<TableResponse> items = tables.stream()
                .map(table -> convertToTableResponse(
                        table, 
                        table.getSchema().getName(), 
                        shareName))
                .collect(Collectors.toList());
        
        return ListTablesResponse.builder()
                .items(items)
                .nextPageToken(null)
                .build();
    }
    
    /**
     * Query table metadata
     * Endpoint: GET /shares/{share}/schemas/{schema}/tables/{table}/metadata
     */
    @Transactional(readOnly = true)
    public String queryTableMetadata(String shareName, String schemaName, String tableName) {
        log.debug("Querying metadata for table: {}.{}.{}", shareName, schemaName, tableName);
        
        verifyShareIsActive(shareName);
        
        DeltaTable table = tableRepository.findByNameAndSchemaNameAndShareName(
                tableName, schemaName, shareName)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Table not found: " + tableName + " in schema: " + schemaName + 
                        " in share: " + shareName));
        
        // Return newline-delimited JSON (NDJSON) format
        StringBuilder response = new StringBuilder();
        
        // Protocol line - SIMPLE format for /metadata endpoint (Databricks compatible)
        ProtocolResponse protocol = ProtocolResponse.builder()
                .minReaderVersion(1)
                .build();
        String protocolJson = String.format("{\"protocol\":%s}", toJson(protocol));
        response.append(protocolJson).append("\n");
        
        // Metadata line - SIMPLE format for /metadata endpoint (Databricks compatible)
        // Generate dynamic schema based on table name and format (delegated to storage service)
        String schemaString = fileStorageService.getTableSchema(table.getName(), table.getFormat());
        
        // Get partition columns (delegated to storage service)
        String[] partCols = fileStorageService.getPartitionColumns(table.getName());
        List<String> partitionColumns = Arrays.asList(partCols);
        
        MetadataResponse metadata = MetadataResponse.builder()
                .id(table.getId().toString())
                .format(FormatResponse.builder()
                        .provider(table.getFormat())
                        .build())
                .schemaString(schemaString)
                .partitionColumns(partitionColumns)
                .configuration(new HashMap<>())
                .build();
        // SIMPLE format (no deltaMetadata wrapper) - Databricks compatible
        String metadataJson = String.format("{\"metaData\":%s}", toJson(metadata));
        response.append(metadataJson).append("\n");
        
        return response.toString();
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
        
        // Protocol line
        // Format for responseformat=parquet: {"protocol": {...}}
        // Format for responseformat=delta: {"protocol": {"deltaProtocol": {...}}}
        ProtocolResponse protocol = ProtocolResponse.builder()
                .minReaderVersion(1)
                .minWriterVersion(1)  // Required by official client
                .build();
        String protocolJson = String.format("{\"protocol\":%s}", toJson(protocol));
        response.append(protocolJson).append("\n");
        
        // Metadata line
        // Format for responseformat=parquet: {"metaData": {...}}
        // Format for responseformat=delta: {"metaData": {"deltaMetadata": {...}}}
        // Generate dynamic schema based on table name and format (delegated to storage service)
        String schemaString = fileStorageService.getTableSchema(table.getName(), table.getFormat());
        
        // Get partition columns (delegated to storage service)
        String[] partCols = fileStorageService.getPartitionColumns(table.getName());
        List<String> partitionColumns = Arrays.asList(partCols);
        
        MetadataResponse metadata = MetadataResponse.builder()
                .id(table.getId().toString())
                .name(table.getName())
                .format(FormatResponse.builder()
                        .provider(table.getFormat())
                        .build())
                .schemaString(schemaString)
                // schema field omitted - not needed as schemaString already contains the schema
                .partitionColumns(partitionColumns)
                .build();
        // Databricks format (responseformat=parquet): {"metaData": {...}}
        String metadataJson = String.format("{\"metaData\":%s}", toJson(metadata));
        response.append(metadataJson).append("\n");
        
        // Get files from storage service
        List<FileResponse> files = fileStorageService.getTableFiles(
                table, 
                request.getVersion(), 
                request.getPredicateHints(), 
                request.getLimitHint()
        );
        
        // Add file lines to response - each wrapped in "file" key
        // Format for responseformat=parquet: {"file": {...}}
        // Format for responseformat=delta: {"file": {"deltaSingleAction": {...}}}
        // Databricks uses parquet format by default
        for (FileResponse file : files) {
            String fileJson = String.format("{\"file\":%s}", toJson(file));
            response.append(fileJson).append("\n");
        }
        
        log.info("Returning {} files for table: {}.{}.{}", files.size(), shareName, schemaName, tableName);
        
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
                .id(share.getId().toString())
                .build();
    }
    
    private SchemaResponse convertToSchemaResponse(DeltaSchema schema, String shareName) {
        return SchemaResponse.builder()
                .name(schema.getName())
                .share(shareName)
                .build();
    }
    
    private TableResponse convertToTableResponse(DeltaTable table, String schemaName, String shareName) {
        return TableResponse.builder()
                .name(table.getName())
                .schema(schemaName)
                .share(shareName)
                .shareAsView(table.getShareAsView())
                .id(table.getId().toString())
                .build();
    }
    
    private String toJson(Object obj) {
        // Simple JSON conversion - in production use Jackson ObjectMapper
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Error converting to JSON", e);
            return "{}";
        }
    }
}
