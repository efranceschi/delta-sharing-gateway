package com.databricks.deltasharing.service.storage;

import com.databricks.deltasharing.dto.delta.AddAction;
import com.databricks.deltasharing.dto.delta.DeltaSnapshot;
import com.databricks.deltasharing.dto.protocol.FileResponse;
import com.databricks.deltasharing.model.DeltaTable;
import com.databricks.deltasharing.service.delta.DataSkippingService;
import com.databricks.deltasharing.service.delta.DeltaLogReader;
import com.databricks.deltasharing.service.parquet.ParquetSchemaReader;
import io.minio.GetObjectArgs;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MinioClient;
import io.minio.http.Method;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * MinIO-based file storage service implementation for Delta Lake tables in S3-compatible storage.
 * 
 * This service:
 * - Reads Delta Lake tables from MinIO/S3 object storage
 * - Extracts bucket name and path from table location (format: s3://bucket-name/path/to/table)
 * - Reads schemas and partition columns from Delta transaction log (_delta_log/*.json) stored in MinIO
 * - Generates pre-signed URLs with configurable expiration for secure file access
 * - Supports Delta Lake transaction log reading and data skipping
 * 
 * Configuration:
 * - endpoint: MinIO/S3 endpoint URL
 * - accessKey/secretKey: S3 credentials
 * - urlExpirationMinutes: Duration for pre-signed URL validity
 * 
 * Note: Bucket name is extracted from table location field (s3://bucket/path), not from configuration
 */
@Service
@Slf4j
@Setter
@ConditionalOnProperty(name = "delta.sharing.storage.type", havingValue = "minio")
@ConfigurationProperties(prefix = "delta.sharing.storage.minio")
public class MinIOFileStorageService implements FileStorageService {
    
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private int urlExpirationMinutes = 60; // Default 1 hour
    private boolean enabled = true;
    private boolean useDeltaLog = true; // Use Delta transaction log by default
    
    // Admin credentials configuration (optional)
    private Admin admin = new Admin();
    
    private MinioClient minioClient;  // Regular client for data access
    private MinioClient adminClient;   // Admin client for cluster metrics (optional)
    
    /**
     * Inner class for admin credentials configuration
     */
    @Setter
    public static class Admin {
        private boolean enabled = false;
        private String accessKey;
        private String secretKey;
        
        public boolean isEnabled() {
            return enabled;
        }
        
        public String getAccessKey() {
            return accessKey;
        }
        
        public String getSecretKey() {
            return secretKey;
        }
        
        public boolean isConfigured() {
            return enabled && accessKey != null && !accessKey.isEmpty() 
                   && secretKey != null && !secretKey.isEmpty();
        }
    }
    
    /**
     * Helper class to hold parsed S3 location information
     */
    private static class S3Location {
        final String bucket;
        final String path;
        
        S3Location(String bucket, String path) {
            this.bucket = bucket;
            this.path = path;
        }
        
        @Override
        public String toString() {
            return String.format("s3://%s/%s", bucket, path);
        }
    }
    
    @Autowired(required = false)
    private DeltaLogReader deltaLogReader;
    
    @Autowired(required = false)
    private DataSkippingService dataSkippingService;
    
    @Autowired(required = false)
    private ParquetSchemaReader parquetSchemaReader;
    
    @Autowired(required = false)
    private com.databricks.deltasharing.repository.DeltaTableRepository tableRepository;
    
    @PostConstruct
    public void init() {
        if (!enabled) {
            log.info("MinIO storage service is disabled");
            return;
        }
        
        if (!isConfigured()) {
            log.warn("MinIO storage service is not properly configured");
            return;
        }
        
        try {
            // Initialize regular client for data access
            minioClient = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, secretKey)
                    .build();
            
            log.info("MinIO client initialized successfully for endpoint: {}", endpoint);
            
            // Initialize admin client if configured
            if (admin.isConfigured()) {
                adminClient = MinioClient.builder()
                        .endpoint(endpoint)
                        .credentials(admin.getAccessKey(), admin.getSecretKey())
                        .build();
                
                log.info("MinIO admin client initialized successfully for endpoint: {}", endpoint);
            } else {
                log.info("MinIO admin client not configured - cluster metrics will not be available");
            }
        } catch (Exception e) {
            log.error("Failed to initialize MinIO client", e);
        }
    }
    
    @Override
    public List<FileResponse> getTableFiles(DeltaTable table, Long version, 
                                             List<String> predicateHints, Integer limitHint) {
        log.debug("Getting MinIO files for table: {} (format: {}) at location: {}", 
                  table.getName(), table.getFormat(), table.getLocation());
        
        if (!isAvailable()) {
            log.warn("MinIO storage service is not available");
            return new ArrayList<>();
        }
        
        // Check if table is Delta format before trying to read Delta Log
        boolean isDeltaTable = "delta".equalsIgnoreCase(table.getFormat());
        
        if (!isDeltaTable) {
            log.info("Table {} is format '{}', not Delta. Skipping Delta log read.", 
                    table.getName(), table.getFormat());
            // For non-Delta tables, use legacy mode directly
            return getTableFilesLegacy(table, version, limitHint);
        }
        
        // Try to use Delta transaction log if enabled (only for Delta tables)
        if (useDeltaLog && deltaLogReader != null && dataSkippingService != null) {
            try {
                return getTableFilesFromDeltaLog(table, version, predicateHints, limitHint);
            } catch (Exception e) {
                log.warn("Failed to read Delta log for table {}, falling back to direct file listing: {}", 
                        table.getName(), e.getMessage());
                log.debug("Delta log error details", e);
            }
        }
        
        // Fallback: list files directly from MinIO (legacy mode, no data skipping)
        log.info("Using legacy mode (no Delta log) for table: {}", table.getName());
        return getTableFilesLegacy(table, version, limitHint);
    }
    
    /**
     * Get table files from Delta transaction log (preferred method)
     * Supports data skipping with predicate pushdown
     */
    private List<FileResponse> getTableFilesFromDeltaLog(DeltaTable table, Long version,
                                                          List<String> predicateHints, Integer limitHint) throws Exception {
        // Parse S3 location to get bucket and path
        S3Location s3Location = resolveTableLocation(table);
        String deltaLogPath = s3Location.path + "_delta_log/";
        
        // Determine which version to read
        Long targetVersion = version != null ? version : findLatestVersion(s3Location.bucket, deltaLogPath);
        String logFileName = String.format("%020d.json", targetVersion);
        String logObjectName = deltaLogPath + logFileName;
        
        log.debug("Reading Delta log from MinIO: {}/{}", s3Location.bucket, logObjectName);
        
        // Read Delta log from MinIO
        DeltaSnapshot snapshot;
        try (InputStream logStream = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(s3Location.bucket)
                        .object(logObjectName)
                        .build())) {
            
            snapshot = deltaLogReader.readDeltaLog(logStream, targetVersion);
        }
        
        log.info("Delta log read: {} files in snapshot (version={}) from {}", 
                 snapshot.getFileCount(), targetVersion, s3Location);
        
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
        
        // Convert AddAction to FileResponse with pre-signed URLs
        return limitedFiles.stream()
                .map(addAction -> createFileResponseFromAddAction(addAction, s3Location, version))
                .collect(Collectors.toList());
    }
    
    /**
     * Legacy method: list files directly from MinIO without Delta log
     * Does NOT support data skipping (predicates are ignored)
     * 
     * @deprecated This method is deprecated and always returns an empty list.
     *             Use Delta transaction log instead (getTableFilesFromDeltaLog)
     */
    @Deprecated
    private List<FileResponse> getTableFilesLegacy(DeltaTable table, Long version, Integer limitHint) {
        log.error("Legacy mode is deprecated. Please ensure Delta transaction log is available for table: {} at {}",
                table.getName(), table.getLocation());
        return new ArrayList<>();
    }
    
    /**
     * Resolve S3 location from table, returning bucket and path
     * 
     * @param table Delta table with location field
     * @return S3Location with bucket and path
     * @throws IllegalArgumentException if table location is invalid
     */
    private S3Location resolveTableLocation(DeltaTable table) {
        String location = table.getLocation();
        
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException(
                "Table location is required but was null or empty for table: " + table.getName());
        }
        
        return parseS3Location(location);
    }
    
    /**
     * Create FileResponse from AddAction (Delta log)
     */
    private FileResponse createFileResponseFromAddAction(AddAction addAction, S3Location s3Location, Long version) {
        try {
            String objectName = s3Location.path + addAction.getPath();
            
            // Generate pre-signed URL
            String presignedUrl = minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(s3Location.bucket)
                            .object(objectName)
                            .expiry(urlExpirationMinutes, TimeUnit.MINUTES)
                            .build()
            );
            
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
            
            long expirationTimestamp = System.currentTimeMillis() + 
                                      TimeUnit.MINUTES.toMillis(urlExpirationMinutes);
            
            return FileResponse.builder()
                    .url(presignedUrl)
                    .id(addAction.getPath().replace("/", "-").replace(".parquet", ""))
                    .partitionValues(addAction.getPartitionValues())
                    .size(addAction.getSize())
                    .stats(stats)
                    .version(version != null ? version : 0L)
                    .timestamp(addAction.getModificationTime() != null ? 
                              addAction.getModificationTime() : System.currentTimeMillis())
                    .expirationTimestamp(expirationTimestamp)
                    .build();
                    
        } catch (Exception e) {
            log.error("Failed to create FileResponse for: {} in {}", addAction.getPath(), s3Location, e);
            return null;
        }
    }
    
    @Override
    public String getStorageType() {
        return "minio";
    }
    
    @Override
    public boolean isAvailable() {
        return enabled && isConfigured() && minioClient != null;
    }
    
    /**
     * Test MinIO connection by listing buckets
     * Used for health checks
     * 
     * @return true if connection is successful, false otherwise
     */
    public boolean testConnection() {
        if (!isAvailable()) {
            return false;
        }
        
        try {
            // Try to list buckets as a simple health check
            minioClient.listBuckets();
            return true;
        } catch (Exception e) {
            log.warn("MinIO connection test failed: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Check if admin client is available
     * 
     * @return true if admin client is configured and initialized
     */
    public boolean isAdminAvailable() {
        return admin.isConfigured() && adminClient != null;
    }
    
    /**
     * Get basic MinIO cluster information
     * Uses regular client for basic info, admin client for detailed metrics if available
     * 
     * @return Map with cluster information
     */
    public Map<String, Object> getClusterInfo() {
        Map<String, Object> info = new HashMap<>();
        
        if (!isAvailable()) {
            info.put("available", false);
            info.put("adminEnabled", false);
            return info;
        }
        
        try {
            // List buckets to get basic cluster info
            var buckets = minioClient.listBuckets();
            info.put("available", true);
            info.put("bucketCount", buckets.size());
            info.put("endpoint", endpoint);
            info.put("adminEnabled", isAdminAvailable());
            
            // If admin client is available, try to get detailed metrics
            if (isAdminAvailable()) {
                try {
                    Map<String, Object> adminInfo = getAdminClusterInfo();
                    info.put("adminInfo", adminInfo);
                } catch (Exception e) {
                    log.warn("Failed to get admin cluster info: {}", e.getMessage());
                    info.put("adminError", "Failed to retrieve admin metrics: " + e.getMessage());
                }
            } else {
                info.put("note", "Admin credentials not configured - detailed metrics unavailable");
            }
            
        } catch (Exception e) {
            log.warn("Failed to get MinIO cluster info: {}", e.getMessage());
            info.put("available", false);
            info.put("error", e.getMessage());
        }
        
        return info;
    }
    
    /**
     * Get detailed cluster information using admin credentials
     * Note: This requires MinIO Admin API which needs additional setup
     * 
     * @return Map with detailed admin cluster information
     */
    private Map<String, Object> getAdminClusterInfo() {
        Map<String, Object> adminInfo = new HashMap<>();
        
        if (!isAdminAvailable()) {
            adminInfo.put("available", false);
            adminInfo.put("message", "Admin client not available");
            return adminInfo;
        }
        
        try {
            // Try to list buckets with admin client to verify permissions
            var buckets = adminClient.listBuckets();
            adminInfo.put("available", true);
            adminInfo.put("bucketsAccessible", buckets.size());
            
            // For detailed cluster metrics (storage, memory, nodes, etc.),
            // we would need to use MinioAdminClient from io.minio:minio-admin
            // which requires additional dependencies:
            // <dependency>
            //     <groupId>io.minio</groupId>
            //     <artifactId>minio-admin</artifactId>
            // </dependency>
            //
            // Example usage with MinioAdminClient:
            // - ServerInfo info = adminClient.serverInfo()
            // - StorageInfo storage = adminClient.storageInfo()
            // - etc.
            
            adminInfo.put("note", "Full admin metrics require MinIO Admin API library (io.minio:minio-admin)");
            
        } catch (Exception e) {
            log.warn("Failed to get admin cluster info: {}", e.getMessage());
            adminInfo.put("available", false);
            adminInfo.put("error", e.getMessage());
        }
        
        return adminInfo;
    }
    
    private boolean isConfigured() {
        if (endpoint == null || endpoint.isEmpty()) {
            log.debug("MinIO endpoint is not configured");
            return false;
        }
        
        if (accessKey == null || accessKey.isEmpty()) {
            log.debug("MinIO access key is not configured");
            return false;
        }
        
        if (secretKey == null || secretKey.isEmpty()) {
            log.debug("MinIO secret key is not configured");
            return false;
        }
        
        return true;
    }
    
    /**
     * Parse S3 location string to extract bucket and path
     * Format: s3://bucket-name/path/to/table
     * 
     * @param location S3 location string
     * @return S3Location object with bucket and path
     * @throws IllegalArgumentException if location format is invalid
     */
    private S3Location parseS3Location(String location) {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException("Table location cannot be null or empty");
        }
        
        // Handle s3:// prefix
        if (!location.startsWith("s3://")) {
            throw new IllegalArgumentException("Table location must start with s3:// - got: " + location);
        }
        
        // Remove s3:// prefix
        String withoutProtocol = location.substring(5);
        
        // Find first slash to separate bucket from path
        int firstSlash = withoutProtocol.indexOf('/');
        
        if (firstSlash < 0) {
            // Only bucket, no path
            String bucket = withoutProtocol;
            log.debug("Parsed S3 location: bucket={}, path=(empty)", bucket);
            return new S3Location(bucket, "");
        }
        
        // Extract bucket and path
        String bucket = withoutProtocol.substring(0, firstSlash);
        String path = withoutProtocol.substring(firstSlash + 1);
        
        // Remove leading slash if present
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        
        // Ensure trailing slash
        if (!path.isEmpty() && !path.endsWith("/")) {
            path = path + "/";
        }
        
        log.debug("Parsed S3 location: bucket={}, path={}", bucket, path);
        return new S3Location(bucket, path);
    }
    
    /**
     * Find the latest version of Delta table by checking _last_checkpoint or listing log files
     * 
     * Strategy:
     * 1. Try to read _last_checkpoint file (contains metadata about latest checkpoint)
     * 2. If not found, list all *.json files in _delta_log/ and find highest version number
     * 3. If nothing found, return 0L as default
     * 
     * @param bucket S3 bucket name
     * @param deltaLogPath Path to _delta_log directory (with trailing slash)
     * @return Latest version number, or 0L if not found
     */
    private Long findLatestVersion(String bucket, String deltaLogPath) {
        try {
            // Strategy 1: Try to read _last_checkpoint file
            String checkpointFile = deltaLogPath + "_last_checkpoint";
            
            try (InputStream checkpointStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucket)
                            .object(checkpointFile)
                            .build())) {
                
                // Parse JSON to get version number
                // Expected format: {"version":123,"size":456}
                String content = new String(checkpointStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                
                // Simple JSON parsing for version field
                java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\"version\"\\s*:\\s*(\\d+)");
                java.util.regex.Matcher matcher = pattern.matcher(content);
                
                if (matcher.find()) {
                    Long version = Long.parseLong(matcher.group(1));
                    log.debug("Found latest version from _last_checkpoint: {}", version);
                    return version;
                }
            } catch (Exception e) {
                log.debug("_last_checkpoint not found or invalid, will list log files: {}", e.getMessage());
            }
            
            // Strategy 2: List all JSON files and find highest version
            log.debug("Listing Delta log files in: {}/{}", bucket, deltaLogPath);
            
            Long maxVersion = null;
            io.minio.ListObjectsArgs listArgs = io.minio.ListObjectsArgs.builder()
                    .bucket(bucket)
                    .prefix(deltaLogPath)
                    .recursive(false)
                    .build();

            log.debug("Listing objects in bucket '{}' with prefix '{}'", bucket, deltaLogPath);

            int fileCount = 0;
            for (io.minio.Result<io.minio.messages.Item> result : minioClient.listObjects(listArgs)) {
                io.minio.messages.Item item = result.get();
                String objectName = item.objectName();

                log.debug("Found object: {}", objectName);

                // Extract filename from full path
                String filename = objectName.substring(deltaLogPath.length());

                log.debug("Extracted filename: {}", filename);

                // Check if it's a version file (e.g., 00000000000000000000.json)
                if (filename.matches("\\d{20}\\.json")) {
                    try {
                        Long version = Long.parseLong(filename.substring(0, 20));
                        log.debug("Extracted version {} from filename {}", version, filename);
                        if (maxVersion == null || version > maxVersion) {
                            log.debug("Updating maxVersion: previous={}, new={}", maxVersion, version);
                            maxVersion = version;
                        }
                    } catch (NumberFormatException e) {
                        log.warn("Failed to parse version from filename: {}", filename);
                    }
                } else {
                    log.debug("Filename '{}' does not match version file pattern", filename);
                }
                fileCount++;
            }

            log.debug("Processed {} files in delta log directory '{}'", fileCount, deltaLogPath);
            if (maxVersion != null) {
                log.info("Found latest version from file listing: {} in {}/{}", maxVersion, bucket, deltaLogPath);
                return maxVersion;
            }
            
            // Strategy 3: Default to version 0 (may be Parquet table without Delta Log)
            log.debug("No Delta log files found in {}/{} (may be Parquet table), defaulting to version 0", bucket, deltaLogPath);
            return 0L;
            
        } catch (Exception e) {
            log.debug("Could not find Delta log version in {}/{} (may be Parquet table): {}", 
                    bucket, deltaLogPath, e.getMessage());
            return 0L;
        }
    }
    
    /**
     * Get table schema from Delta log metadata
     * For MinIO, we read the schema from the Delta transaction log stored in MinIO
     * Cached to avoid repeated MinIO reads and Delta log parsing
     */
    @Override
    @Cacheable(value = "tableSchemas", key = "#tableName + '_' + #format")
    public String getTableSchema(String tableName, String format) {
        log.debug("Reading (uncached) schema for table: {} (format: {}) from MinIO", tableName, format);
        
        if (!isAvailable()) {
            log.warn("MinIO storage service is not available");
            throw new IllegalStateException("MinIO storage service is not available");
        }
        
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
     * 
     * For non-Delta tables, falls back to Parquet schema reading
     */
    private String readDeltaTableSchema(String tableName) {
        // Check if this is actually a Delta table
        DeltaTable tableEntity = findTableByName(tableName);
        if (tableEntity != null && !"delta".equalsIgnoreCase(tableEntity.getFormat())) {
            log.info("Table {} is format '{}', not Delta. Using Parquet schema reader.", 
                    tableName, tableEntity.getFormat());
            return readParquetTableSchema(tableName);
        }
        
        if (deltaLogReader == null) {
            log.warn("DeltaLogReader not available, cannot read Delta schema");
            throw new IllegalStateException("DeltaLogReader is required for Delta tables");
        }
        
        try {
            // Parse S3 location from table name (assuming format s3://bucket/path/tablename)
            S3Location s3Location = parseS3LocationFromTableName(tableName);
            String deltaLogPath = s3Location.path + "_delta_log/";
            
            // Find the latest version
            Long targetVersion = findLatestVersion(s3Location.bucket, deltaLogPath);
            
            // If no version found, table may not be Delta format
            if (targetVersion == 0L) {
                log.info("No Delta log found for table: {}. Falling back to Parquet schema.", tableName);
                return readParquetTableSchema(tableName);
            }
            
            String logFileName = String.format("%020d.json", targetVersion);
            String logObjectName = deltaLogPath + logFileName;
            
            log.debug("Reading Delta log schema from MinIO: {}/{}", s3Location.bucket, logObjectName);
            
            // Read Delta log from MinIO
            DeltaSnapshot snapshot;
            try (InputStream logStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(s3Location.bucket)
                            .object(logObjectName)
                            .build())) {
                
                snapshot = deltaLogReader.readDeltaLog(logStream, targetVersion);
            }
            
            if (snapshot.getMetadata() != null && snapshot.getMetadata().getSchemaString() != null) {
                log.debug("Schema read from Delta log in MinIO for table: {}", tableName);
                return snapshot.getMetadata().getSchemaString();
            } else {
                log.warn("No schema found in Delta log for table: {}", tableName);
                throw new IllegalStateException("Schema not found in Delta log for table: " + tableName);
            }
            
        } catch (Exception e) {
            log.warn("Failed to read schema from Delta log for table: {}. Trying Parquet fallback: {}", 
                    tableName, e.getMessage());
            // Fallback to Parquet schema if Delta log read fails
            return readParquetTableSchema(tableName);
        }
    }
    
    /**
     * Read schema from Parquet table (without Delta Log)
     * Reads schema from actual Parquet file in MinIO
     */
    private String readParquetTableSchema(String tableName) {
        if (parquetSchemaReader == null) {
            log.warn("ParquetSchemaReader not available, using basic schema for table: {}", tableName);
            return generateBasicParquetSchema(tableName);
        }
        
        if (!isAvailable()) {
            log.warn("MinIO storage service is not available, using basic schema for table: {}", tableName);
            return generateBasicParquetSchema(tableName);
        }
        
        try {
            // Parse S3 location from table name
            S3Location s3Location = parseS3LocationFromTableName(tableName);
            
            // List objects in the table directory to find a Parquet file
            io.minio.ListObjectsArgs listArgs = io.minio.ListObjectsArgs.builder()
                    .bucket(s3Location.bucket)
                    .prefix(s3Location.path)
                    .recursive(false)
                    .build();
            
            String firstParquetObject = null;
            for (io.minio.Result<io.minio.messages.Item> result : minioClient.listObjects(listArgs)) {
                io.minio.messages.Item item = result.get();
                if (item.objectName().endsWith(".parquet")) {
                    firstParquetObject = item.objectName();
                    break;
                }
            }
            
            if (firstParquetObject == null) {
                log.warn("No Parquet files found in MinIO for table: {} at {}. Using basic schema.", 
                        tableName, s3Location);
                return generateBasicParquetSchema(tableName);
            }
            
            // Read Parquet file from MinIO to get schema
            log.info("Reading Parquet schema from MinIO: {}/{}", s3Location.bucket, firstParquetObject);
            
            try (InputStream parquetStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(s3Location.bucket)
                            .object(firstParquetObject)
                            .build())) {
                
                return parquetSchemaReader.readSchemaFromStream(parquetStream, firstParquetObject);
            }
            
        } catch (IOException e) {
            log.error("Failed to read Parquet schema for table: {}. Falling back to basic schema.", tableName, e);
            return generateBasicParquetSchema(tableName);
        } catch (Exception e) {
            log.error("MinIO error while reading Parquet schema for table: {}. Falling back to basic schema.", 
                    tableName, e);
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
              ]
            }
            """;
    }
    
    /**
     * Get partition columns from Delta log metadata
     * For MinIO, we read partition info from the Delta transaction log stored in MinIO
     * Cached to avoid repeated MinIO reads and Delta log parsing
     * 
     * For Parquet tables without Delta Log, returns empty array
     */
    @Override
    @Cacheable(value = "partitionColumns", key = "#tableName")
    public String[] getPartitionColumns(String tableName) {
        log.debug("Reading (uncached) partition columns for table: {} from Delta log in MinIO", tableName);
        
        // Check if this is a Delta table by looking up in repository
        DeltaTable tableEntity = findTableByName(tableName);
        if (tableEntity != null && !"delta".equalsIgnoreCase(tableEntity.getFormat())) {
            log.debug("Table {} is format '{}', not Delta. Partition columns not available.", 
                    tableName, tableEntity.getFormat());
            return new String[0];
        }
        
        if (deltaLogReader == null) {
            log.warn("DeltaLogReader not available, cannot read partition columns");
            return new String[0];
        }
        
        if (!isAvailable()) {
            log.warn("MinIO storage service is not available");
            return new String[0];
        }
        
        try {
            // Parse S3 location from table name
            S3Location s3Location = parseS3LocationFromTableName(tableName);
            String deltaLogPath = s3Location.path + "_delta_log/";
            
            // Find the latest version
            Long targetVersion = findLatestVersion(s3Location.bucket, deltaLogPath);
            
            // If no version found, likely not a Delta table
            if (targetVersion == 0L) {
                log.debug("No Delta log version found for table: {}. May not be a Delta table.", tableName);
                return new String[0];
            }
            
            String logFileName = String.format("%020d.json", targetVersion);
            String logObjectName = deltaLogPath + logFileName;
            
            log.debug("Reading Delta log partition columns from MinIO: {}/{}", s3Location.bucket, logObjectName);
            
            // Read Delta log from MinIO
            DeltaSnapshot snapshot;
            try (InputStream logStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(s3Location.bucket)
                            .object(logObjectName)
                            .build())) {
                
                snapshot = deltaLogReader.readDeltaLog(logStream, targetVersion);
            }
            
            if (snapshot.getMetadata() != null && snapshot.getMetadata().getPartitionColumns() != null) {
                List<String> partitionColumns = snapshot.getMetadata().getPartitionColumns();
                log.debug("Partition columns read from Delta log in MinIO for table {}: {}", 
                         tableName, partitionColumns);
                return partitionColumns.toArray(new String[0]);
            } else {
                log.debug("No partition columns found in Delta log for table: {}", tableName);
                return new String[0];
            }
            
        } catch (Exception e) {
            log.debug("Could not read partition columns from Delta log for table: {} (may be Parquet table): {}", 
                    tableName, e.getMessage());
            return new String[0];
        }
    }
    
    /**
     * Helper method to find a table by name in the repository
     * 
     * @param tableName Table name to search for
     * @return DeltaTable entity or null if not found
     */
    private DeltaTable findTableByName(String tableName) {
        if (tableRepository == null) {
            return null;
        }
        
        try {
            // Find all tables with this name (there might be multiple in different schemas/shares)
            java.util.List<DeltaTable> tables = tableRepository.findAll().stream()
                    .filter(t -> tableName.equals(t.getName()))
                    .collect(java.util.stream.Collectors.toList());
            
            if (!tables.isEmpty()) {
                if (tables.size() > 1) {
                    log.debug("Found {} tables with name '{}', using first one", tables.size(), tableName);
                }
                return tables.get(0);
            }
        } catch (Exception e) {
            log.debug("Error looking up table '{}' in repository: {}", tableName, e.getMessage());
        }
        
        return null;
    }
    
    /**
     * Parse S3 location from table name
     * 
     * Strategy:
     * 1. If tableName is a full S3 location (starts with s3://), parse it directly
     * 2. Otherwise, try to find the table in the repository and use its location field
     * 3. If not found, throw an exception
     * 
     * @param tableName Table name or full S3 location
     * @return S3Location with bucket and path
     * @throws IllegalArgumentException if table cannot be found or has no location
     */
    private S3Location parseS3LocationFromTableName(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        
        // Strategy 1: Check if tableName is actually a full S3 location
        if (tableName.startsWith("s3://")) {
            return parseS3Location(tableName);
        }
        
        // Strategy 2: Try to find the table in the repository by name
        DeltaTable table = findTableByName(tableName);
        if (table != null) {
            String location = table.getLocation();
            if (location != null && !location.isEmpty()) {
                log.debug("Found table location in repository: {}", location);
                return parseS3Location(location);
            } else {
                throw new IllegalArgumentException(
                    "Table '" + tableName + "' found in repository but has no location configured");
            }
        }
        
        log.warn("Table '{}' not found in repository", tableName);
        
        // Strategy 3: If we reach here, we couldn't resolve the location
        throw new IllegalArgumentException(
            "Cannot resolve S3 location for table '" + tableName + "'. " +
            "Please ensure the table exists in the database with a valid location field (s3://bucket/path)");
    }
}
