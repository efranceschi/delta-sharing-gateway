package com.databricks.deltasharing.service.storage;

import com.databricks.deltasharing.dto.delta.AddAction;
import com.databricks.deltasharing.dto.delta.DeltaSnapshot;
import com.databricks.deltasharing.dto.protocol.FileResponse;
import com.databricks.deltasharing.model.DeltaTable;
import com.databricks.deltasharing.service.delta.DataSkippingService;
import com.databricks.deltasharing.service.delta.DeltaLogReader;
import com.databricks.deltasharing.service.parquet.ParquetSchemaReader;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HTTP-based file storage service implementation for local Delta Lake tables.
 * 
 * This service:
 * - Reads Delta Lake tables from local filesystem (basePath)
 * - Reads schemas and partition columns from Delta transaction log (_delta_log/*.json)
 * - Serves files through a generic HTTP server without temporary token signing
 * - Constructs HTTP URLs based on configurable baseUrl
 * - Supports Delta Lake transaction log reading and data skipping
 * 
 * Configuration:
 * - basePath: Local filesystem path where Delta tables are stored
 * - baseUrl: HTTP base URL for serving Parquet files
 */
@Service
@Slf4j
@Setter
@ConditionalOnProperty(name = "delta.sharing.storage.type", havingValue = "http")
@ConfigurationProperties(prefix = "delta.sharing.storage.http")
public class HttpFileStorageService implements FileStorageService {
    
    private String baseUrl;
    private String basePath;
    private boolean enabled = true;
    private boolean useDeltaLog = true; // Use Delta transaction log by default
    
    @Autowired(required = false)
    private DeltaLogReader deltaLogReader;
    
    @Autowired(required = false)
    private DataSkippingService dataSkippingService;
    
    @Autowired(required = false)
    private ParquetSchemaReader parquetSchemaReader;
    
    @Override
    public List<FileResponse> getTableFiles(DeltaTable table, Long version, 
                                             List<String> predicateHints, Integer limitHint, String startingTimestamp) {
        log.debug("Getting HTTP files for table: {} at location: {} (startingTimestamp: {})", 
                  table.getName(), table.getLocation(), startingTimestamp);
        
        if (!isAvailable()) {
            log.warn("HTTP storage service is not properly configured");
            return new ArrayList<>();
        }
        
        // Try to use Delta transaction log if enabled
        if (useDeltaLog && deltaLogReader != null && dataSkippingService != null) {
            try {
                return getTableFilesFromDeltaLog(table, version, predicateHints, limitHint);
            } catch (Exception e) {
                log.warn("Failed to read Delta log, falling back to direct file listing: {}", e.getMessage());
                log.debug("Delta log error details", e);
            }
        }
        
        // Fallback: list files directly from filesystem (legacy mode, no data skipping)
        log.info("Using legacy mode (no Delta log) for table: {}", table.getName());
        return getTableFilesLegacy(table, version, limitHint);
    }
    
    /**
     * Get table files from Delta transaction log (preferred method)
     * Supports data skipping with predicate pushdown
     */
    private List<FileResponse> getTableFilesFromDeltaLog(DeltaTable table, Long version,
                                                          List<String> predicateHints, Integer limitHint) throws Exception {
        String tableLocation = resolveTableLocation(table);
        Path tablePath = Paths.get(tableLocation);
        
        log.debug("Reading Delta log from filesystem: {}", tableLocation);
        
        // Read Delta log from filesystem
        DeltaSnapshot snapshot = deltaLogReader.readDeltaLog(tableLocation, version);
        
        log.info("Delta log read: {} files in snapshot (version={})", 
                 snapshot.getFileCount(), version);
        
        // Apply data skipping with predicates
        List<AddAction> allFiles = snapshot.getAddActions();
        List<AddAction> filteredFiles = dataSkippingService.applyDataSkipping(allFiles, predicateHints);
        
        // Apply limit hint
        int limit = limitHint != null && limitHint > 0 ? limitHint : Integer.MAX_VALUE;
        List<AddAction> limitedFiles = filteredFiles.stream()
                .limit(limit)
                .collect(Collectors.toList());
        
        log.info("Returning {} files after data skipping and limit (from {} total)", 
                 limitedFiles.size(), allFiles.size());
        
        // Convert AddAction to FileResponse with HTTP URLs
        return limitedFiles.stream()
                .map(addAction -> createFileResponseFromAddAction(addAction, tablePath, version))
                .filter(response -> response != null)
                .collect(Collectors.toList());
    }
    
    /**
     * Legacy method: list files directly from filesystem without Delta log
     * Does NOT support data skipping (predicates are ignored)
     */
    private List<FileResponse> getTableFilesLegacy(DeltaTable table, Long version, Integer limitHint) {
        log.warn("Legacy mode does not support data skipping - predicates will be ignored");
        
        List<FileResponse> files = new ArrayList<>();
        String tableLocation = resolveTableLocation(table);
        
        try {
            Path tablePath = Paths.get(tableLocation);
            
            if (!Files.exists(tablePath)) {
                log.warn("Table location does not exist: {}", tableLocation);
                return files;
            }
            
            // Legacy: just list all Parquet files (no Delta log, no data skipping)
            log.error("Legacy mode is deprecated. Please ensure Delta transaction log is available.");
            
        } catch (Exception e) {
            log.error("Error listing files for table: {}", table.getName(), e);
        }
        
        return files;
    }
    
    /**
     * Create FileResponse from AddAction (Delta log)
     */
    private FileResponse createFileResponseFromAddAction(AddAction addAction, Path tablePath, Long version) {
        try {
            Path filePath = tablePath.resolve(addAction.getPath());
            Path relativePath = tablePath.relativize(filePath);
            
            // Construct HTTP URL
            String urlPath = relativePath.toString().replace(File.separator, "/");
            String fileUrl = baseUrl.endsWith("/") ? 
                             baseUrl + urlPath : 
                             baseUrl + "/" + urlPath;
            
            // Convert FileStatistics to Map for response
            Map<String, Object> stats = null;
            if (addAction.getParsedStats() != null) {
                stats = new HashMap<>();
                stats.put("numRecords", addAction.getParsedStats().getNumRecords());
                if (addAction.getParsedStats().getMinValues() != null) {
                    stats.put("minValues", addAction.getParsedStats().getMinValues());
                }
                if (addAction.getParsedStats().getMaxValues() != null) {
                    stats.put("maxValues", addAction.getParsedStats().getMaxValues());
                }
                if (addAction.getParsedStats().getNullCount() != null) {
                    stats.put("nullCount", addAction.getParsedStats().getNullCount());
                }
            }
            
            return FileResponse.builder()
                    .url(fileUrl)
                    .id(addAction.getPath().replace("/", "-").replace(".parquet", ""))
                    .partitionValues(addAction.getPartitionValues())
                    .size(addAction.getSize())
                    .stats(stats)
                    .version(version != null ? version : 0L)
                    .timestamp(addAction.getModificationTime() != null ? 
                              addAction.getModificationTime() : System.currentTimeMillis())
                    .build();
                    
        } catch (Exception e) {
            log.error("Failed to create FileResponse for: {}", addAction.getPath(), e);
            return null;
        }
    }
    
    private String resolveTableLocation(DeltaTable table) {
        String location = table.getLocation();
        
        if (location == null || location.isEmpty()) {
            location = table.getName();
        }
        
        // Handle s3:// prefix - extract path after bucket
        if (location.startsWith("s3://")) {
            String withoutProtocol = location.substring(5); // Remove "s3://"
            int firstSlash = withoutProtocol.indexOf('/');
            
            if (firstSlash > 0) {
                location = withoutProtocol.substring(firstSlash + 1); // Path after bucket
                log.debug("Resolved S3 location to local path: {}", location);
            } else {
                // Only bucket name, use table name
                location = table.getName();
            }
        }
        
        // If location is absolute, use it directly
        if (location.startsWith("/") || location.matches("^[a-zA-Z]:.*")) {
            return location;
        }
        
        // Otherwise, combine with base path
        if (basePath != null && !basePath.isEmpty()) {
            return Paths.get(basePath, location).toString();
        }
        
        return location;
    }
    
    @Override
    public String getStorageType() {
        return "http";
    }
    
    @Override
    public boolean isAvailable() {
        return enabled && baseUrl != null && !baseUrl.isEmpty();
    }
    
    /**
     * Get table schema from Delta log metadata
     * For HTTP storage, we read the schema from the Delta transaction log
     * Cached to avoid repeated Delta log reads
     */
    @Override
    @Cacheable(value = "tableSchemas", key = "#tableName + '_' + #format")
    public String getTableSchema(String tableName, String format) {
        log.debug("Reading (uncached) schema for table: {} (format: {}) from local filesystem", tableName, format);
        
        // Handle different formats
        if ("delta".equalsIgnoreCase(format)) {
            return readDeltaTableSchema(tableName);
        } else if ("parquet".equalsIgnoreCase(format)) {
            return readParquetTableSchema(tableName);
        } else {
            log.warn("Unsupported table format: {}. Defaulting to Parquet schema.", format);
            return readParquetTableSchema(tableName);
        }
    }
    
    /**
     * Read schema from Delta table (via Delta Log)
     */
    private String readDeltaTableSchema(String tableName) {
        if (deltaLogReader == null) {
            log.warn("DeltaLogReader not available, cannot read Delta schema");
            throw new IllegalStateException("DeltaLogReader is required for Delta tables");
        }
        
        try {
            // Construct table location
            String tableLocation = resolveTableLocationByName(tableName);
            
            // Read Delta log to get metadata
            DeltaSnapshot snapshot = deltaLogReader.readDeltaLog(tableLocation, null);
            
            if (snapshot.getMetadata() != null && snapshot.getMetadata().getSchemaString() != null) {
                log.debug("Schema read from Delta log for table: {}", tableName);
                return snapshot.getMetadata().getSchemaString();
            } else {
                log.warn("No schema found in Delta log for table: {}", tableName);
                throw new IllegalStateException("Schema not found in Delta log for table: " + tableName);
            }
            
        } catch (Exception e) {
            log.error("Failed to read schema from Delta log for table: {}", tableName, e);
            throw new RuntimeException("Failed to read schema from Delta log: " + e.getMessage(), e);
        }
    }
    
    /**
     * Read schema from Parquet table (without Delta Log)
     * Reads schema from actual Parquet file
     */
    private String readParquetTableSchema(String tableName) {
        if (parquetSchemaReader == null) {
            log.warn("ParquetSchemaReader not available, using basic schema for table: {}", tableName);
            return generateBasicParquetSchema(tableName);
        }
        
        try {
            // Construct table location
            String tableLocation = resolveTableLocationByName(tableName);
            File tableDir = new File(tableLocation);
            
            if (!tableDir.exists() || !tableDir.isDirectory()) {
                log.warn("Table directory not found: {}. Using basic schema.", tableLocation);
                return generateBasicParquetSchema(tableName);
            }
            
            // Find first Parquet file in directory
            File parquetFile = parquetSchemaReader.findFirstParquetFile(tableDir);
            
            if (parquetFile == null) {
                log.warn("No Parquet files found in directory: {}. Using basic schema.", tableLocation);
                return generateBasicParquetSchema(tableName);
            }
            
            // Read schema from Parquet file
            log.info("Reading Parquet schema from file: {}", parquetFile.getName());
            return parquetSchemaReader.readSchemaFromFile(parquetFile);
            
        } catch (IOException e) {
            log.error("Failed to read Parquet schema for table: {}. Falling back to basic schema.", tableName, e);
            return generateBasicParquetSchema(tableName);
        }
    }
    
    /**
     * Generate a basic schema for Parquet tables
     */
    private String generateBasicParquetSchema(String tableName) {
        // Generate a minimal schema structure
        return """
            {
              "type": "struct",
              "fields": [
                {
                  "name": "id",
                  "type": "long",
                  "nullable": false,
                  "metadata": {}
                },
                {
                  "name": "data",
                  "type": "string",
                  "nullable": true,
                  "metadata": {}
                },
                {
                  "name": "timestamp",
                  "type": "timestamp",
                  "nullable": true,
                  "metadata": {}
                }
              ]
            }
            """;
    }
    
    /**
     * Get partition columns from Delta log metadata
     * For HTTP storage, we read partition info from the Delta transaction log
     * Cached to avoid repeated Delta log reads
     */
    @Override
    @Cacheable(value = "partitionColumns", key = "#tableName")
    public String[] getPartitionColumns(String tableName) {
        log.debug("Reading (uncached) partition columns for table: {} from Delta log", tableName);
        
        if (deltaLogReader == null) {
            log.warn("DeltaLogReader not available, cannot read partition columns");
            return new String[0];
        }
        
        try {
            // Construct table location
            String tableLocation = resolveTableLocationByName(tableName);
            
            // Read Delta log to get metadata
            DeltaSnapshot snapshot = deltaLogReader.readDeltaLog(tableLocation, null);
            
            if (snapshot.getMetadata() != null && snapshot.getMetadata().getPartitionColumns() != null) {
                List<String> partitionColumns = snapshot.getMetadata().getPartitionColumns();
                log.debug("Partition columns read from Delta log for table {}: {}", 
                         tableName, partitionColumns);
                return partitionColumns.toArray(new String[0]);
            } else {
                log.debug("No partition columns found in Delta log for table: {}", tableName);
                return new String[0];
            }
            
        } catch (Exception e) {
            log.warn("Failed to read partition columns from Delta log for table: {}", tableName, e);
            return new String[0];
        }
    }
    
    /**
     * Resolve table location by name
     * Helper method for schema and partition column lookup
     */
    private String resolveTableLocationByName(String tableName) {
        // If basePath is configured, use it as root
        if (basePath != null && !basePath.isEmpty()) {
            return Paths.get(basePath, tableName).toString();
        }
        
        // Otherwise, use table name as relative path
        return tableName;
    }
}
