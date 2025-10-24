package com.databricks.deltasharing.service;

import com.databricks.deltasharing.config.TableCrawlerProperties;
import com.databricks.deltasharing.config.StorageConfigProperties;
import com.databricks.deltasharing.model.CrawlerExecution;
import com.databricks.deltasharing.model.DeltaSchema;
import com.databricks.deltasharing.model.DeltaShare;
import com.databricks.deltasharing.model.DeltaTable;
import com.databricks.deltasharing.repository.CrawlerExecutionRepository;
import com.databricks.deltasharing.repository.DeltaSchemaRepository;
import com.databricks.deltasharing.repository.DeltaShareRepository;
import com.databricks.deltasharing.repository.DeltaTableRepository;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Automatic table crawler service that discovers new Delta/Parquet tables
 * in configured storages based on a configurable directory pattern.
 * 
 * Runs periodically based on configured interval.
 */
@Service
@ConditionalOnProperty(name = "delta.sharing.crawler.enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class TableCrawlerService {
    
    private final TableCrawlerProperties crawlerProperties;
    private final StorageConfigProperties storageConfig;
    private final DeltaShareRepository shareRepository;
    private final DeltaSchemaRepository schemaRepository;
    private final DeltaTableRepository tableRepository;
    private final CrawlerExecutionRepository executionRepository;
    
    /**
     * Scheduled task that runs the table crawler
     * Interval is configured in application.yml (delta.sharing.crawler.interval-minutes)
     */
    @Scheduled(
        fixedDelayString = "${delta.sharing.crawler.interval-minutes:5}",
        timeUnit = TimeUnit.MINUTES,
        initialDelayString = "${delta.sharing.crawler.initial-delay-minutes:1}"
    )
    public void crawlTables() {
        if (!crawlerProperties.isEnabled()) {
            log.debug("Table crawler is disabled, skipping execution");
            return;
        }
        
        // Create execution record
        CrawlerExecution execution = CrawlerExecution.builder()
                .startedAt(LocalDateTime.now())
                .status(CrawlerExecution.ExecutionStatus.RUNNING)
                .storageType(storageConfig.getType())
                .discoveryPattern(crawlerProperties.getDiscoveryPattern())
                .dryRun(crawlerProperties.isDryRun())
                .build();
        execution = executionRepository.save(execution);
        
        log.info("╔════════════════════════════════════════════════════════════════");
        log.info("║ Starting Automatic Table Crawler (Execution #{})", execution.getId());
        log.info("╠════════════════════════════════════════════════════════════════");
        log.info("║ Pattern: {}", crawlerProperties.getDiscoveryPattern());
        log.info("║ Dry-run: {}", crawlerProperties.isDryRun());
        log.info("║ Auto-create schemas: {}", crawlerProperties.isAutoCreateSchemas());
        log.info("║ Storage type: {}", storageConfig.getType());
        log.info("╚════════════════════════════════════════════════════════════════");
        
        long startTime = System.currentTimeMillis();
        int discoveredTables = 0;
        int createdTables = 0;
        int createdSchemas = 0;
        
        try {
            // Get all active shares
            List<DeltaShare> activeShares = shareRepository.findAll().stream()
                    .filter(DeltaShare::getActive)
                    .toList();
            
            log.info("Found {} active share(s) to scan", activeShares.size());
            
            for (DeltaShare share : activeShares) {
                log.debug("Processing share: {}", share.getName());
                
                // Scan storage for tables in this share
                CrawlerResult result = scanStorageForShare(share);
                
                discoveredTables += result.discoveredTables;
                createdTables += result.createdTables;
                createdSchemas += result.createdSchemas;
            }
            
            long duration = System.currentTimeMillis() - startTime;
            
            // Update execution record with success
            execution.setFinishedAt(LocalDateTime.now());
            execution.setDurationMs(duration);
            execution.setDiscoveredTables(discoveredTables);
            execution.setCreatedSchemas(createdSchemas);
            execution.setCreatedTables(createdTables);
            execution.setStatus(CrawlerExecution.ExecutionStatus.SUCCESS);
            executionRepository.save(execution);
            
            log.info("╔════════════════════════════════════════════════════════════════");
            log.info("║ Table Crawler Completed (Execution #{})", execution.getId());
            log.info("╠════════════════════════════════════════════════════════════════");
            log.info("║ Discovered tables: {}", discoveredTables);
            log.info("║ Created schemas:   {}", createdSchemas);
            log.info("║ Created tables:    {}", createdTables);
            log.info("║ Duration:          {} ms", duration);
            log.info("╚════════════════════════════════════════════════════════════════");
            
        } catch (Exception e) {
            log.error("Error during table crawler execution", e);
            
            // Update execution record with failure
            execution.setFinishedAt(LocalDateTime.now());
            execution.setDurationMs(System.currentTimeMillis() - startTime);
            execution.setStatus(CrawlerExecution.ExecutionStatus.FAILED);
            execution.setErrorMessage(e.getMessage());
            execution.setDiscoveredTables(discoveredTables);
            execution.setCreatedSchemas(createdSchemas);
            execution.setCreatedTables(createdTables);
            executionRepository.save(execution);
        }
    }
    
    /**
     * Scan configured storage for tables in a given share
     */
    private CrawlerResult scanStorageForShare(DeltaShare share) {
        CrawlerResult result = new CrawlerResult();
        
        try {
            // Parse discovery pattern
            DiscoveryPattern pattern = parseDiscoveryPattern(crawlerProperties.getDiscoveryPattern());
            
            // Based on storage type, scan for tables
            String storageType = storageConfig.getType().toUpperCase();
            
            switch (storageType) {
                case "MINIO":
                    result = scanMinIOStorage(share, pattern);
                    break;
                    
                case "HTTP":
                    result = scanHttpStorage(share, pattern);
                    break;
                    
                case "FILESYSTEM":
                    result = scanFilesystemStorage(share, pattern);
                    break;
                    
                case "FAKE":
                    log.debug("Skipping FAKE storage type (no real data to discover)");
                    break;
                    
                default:
                    log.warn("Unsupported storage type for crawler: {}", storageType);
            }
            
        } catch (Exception e) {
            log.error("Error scanning storage for share: {}", share.getName(), e);
        }
        
        return result;
    }
    
    /**
     * Scan MinIO/S3 storage for tables
     */
    private CrawlerResult scanMinIOStorage(DeltaShare share, DiscoveryPattern pattern) {
        CrawlerResult result = new CrawlerResult();
        
        try {
            // Build MinIO client from configuration
            MinioClient minioClient = MinioClient.builder()
                    .endpoint(storageConfig.getMinio().getEndpoint())
                    .credentials(storageConfig.getMinio().getAccessKey(), storageConfig.getMinio().getSecretKey())
                    .build();
            
            String bucket = storageConfig.getMinio().getBucket();
            log.debug("Scanning MinIO bucket: {} on endpoint: {}", bucket, storageConfig.getMinio().getEndpoint());
            
            // List all objects in bucket
            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(bucket)
                            .recursive(true)
                            .build()
            );
            
            // Track discovered paths
            Set<String> discoveredTables = new HashSet<>();
            
            for (Result<Item> itemResult : results) {
                try {
                    Item item = itemResult.get();
                    String objectPath = item.objectName();
                    
                    // Check if this is a Delta table (_delta_log directory)
                    if (objectPath.contains("_delta_log/")) {
                        String tablePath = extractTablePathFromDeltaLog(objectPath);
                        if (tablePath != null && !discoveredTables.contains(tablePath)) {
                            discoveredTables.add(tablePath);
                            
                            // Build full S3 path
                            String fullPath = String.format("s3://%s/%s", bucket, tablePath);
                            
                            // Extract schema and table from path using pattern
                            TableInfo tableInfo = extractTableInfo(fullPath, pattern, bucket, share.getName());
                            
                            if (tableInfo != null) {
                                result.discoveredTables++;
                                log.debug("Discovered Delta table: {} in schema: {}", tableInfo.tableName, tableInfo.schemaName);
                                
                                // Create schema if needed
                                DeltaSchema schema = getOrCreateSchema(tableInfo.schemaName, share);
                                if (schema != null) {
                                    if (tableInfo.isNewSchema) {
                                        result.createdSchemas++;
                                    }
                                    
                                    // Create table if not exists
                                    if (createTableIfNotExists(tableInfo.tableName, fullPath, "delta", schema)) {
                                        result.createdTables++;
                                    }
                                }
                            }
                        }
                    }
                    // Check for Parquet files
                    else if (objectPath.endsWith(".parquet") && !objectPath.contains("_delta_log/")) {
                        String tablePath = extractTablePathFromParquet(objectPath);
                        if (tablePath != null && !discoveredTables.contains(tablePath)) {
                            discoveredTables.add(tablePath);
                            
                            String fullPath = String.format("s3://%s/%s", bucket, tablePath);
                            TableInfo tableInfo = extractTableInfo(fullPath, pattern, bucket, share.getName());
                            
                            if (tableInfo != null) {
                                result.discoveredTables++;
                                log.debug("Discovered Parquet table: {} in schema: {}", tableInfo.tableName, tableInfo.schemaName);
                                
                                DeltaSchema schema = getOrCreateSchema(tableInfo.schemaName, share);
                                if (schema != null) {
                                    if (tableInfo.isNewSchema) {
                                        result.createdSchemas++;
                                    }
                                    
                                    if (createTableIfNotExists(tableInfo.tableName, fullPath, "parquet", schema)) {
                                        result.createdTables++;
                                    }
                                }
                            }
                        }
                    }
                    
                } catch (Exception e) {
                    log.warn("Error processing object in MinIO: {}", e.getMessage());
                }
            }
            
            log.info("Completed MinIO scan: {} tables discovered", discoveredTables.size());
            
        } catch (Exception e) {
            log.error("Error scanning MinIO storage for share: {}", share.getName(), e);
        }
        
        return result;
    }
    
    /**
     * Scan HTTP storage for tables
     */
    private CrawlerResult scanHttpStorage(DeltaShare share, DiscoveryPattern pattern) {
        CrawlerResult result = new CrawlerResult();
        
        // Scan local filesystem path (httpBasePath)
        String basePath = storageConfig.getHttp().getBasePath();
        if (basePath == null || basePath.isEmpty()) {
            log.warn("HTTP storage has no base path configured");
            return result;
        }
        
        Path rootPath = Paths.get(basePath);
        if (!Files.exists(rootPath)) {
            log.warn("HTTP storage base path does not exist: {}", basePath);
            return result;
        }
        
        log.debug("Scanning HTTP storage filesystem: {}", basePath);
        
        try {
            result = scanFilesystemPath(rootPath, share, pattern, storageConfig.getHttp().getBaseUrl(), false);
        } catch (Exception e) {
            log.error("Error scanning HTTP storage for share: {}", share.getName(), e);
        }
        
        return result;
    }
    
    /**
     * Scan Filesystem storage for tables
     */
    private CrawlerResult scanFilesystemStorage(DeltaShare share, DiscoveryPattern pattern) {
        CrawlerResult result = new CrawlerResult();
        
        String basePath = storageConfig.getFilesystem().getBasePath();
        if (basePath == null || basePath.isEmpty()) {
            log.warn("Filesystem storage has no base path configured");
            return result;
        }
        
        Path rootPath = Paths.get(basePath);
        if (!Files.exists(rootPath)) {
            log.warn("Filesystem storage base path does not exist: {}", basePath);
            return result;
        }
        
        log.debug("Scanning filesystem storage: {}", basePath);
        
        try {
            result = scanFilesystemPath(rootPath, share, pattern, null, true);
        } catch (Exception e) {
            log.error("Error scanning filesystem storage for share: {}", share.getName(), e);
        }
        
        return result;
    }
    
    /**
     * Common method to scan filesystem paths (used by both HTTP and Filesystem storages)
     */
    private CrawlerResult scanFilesystemPath(Path rootPath, DeltaShare share, DiscoveryPattern pattern, 
                                             String baseUrl, boolean useFileProtocol) throws IOException {
        CrawlerResult result = new CrawlerResult();
        Set<String> discoveredTables = new HashSet<>();
        
        // Walk directory tree
        try (Stream<Path> paths = Files.walk(rootPath, crawlerProperties.getMaxScanDepth())) {
            paths.filter(Files::isDirectory)
                .forEach(dirPath -> {
                    try {
                        // Check if this is a Delta table (contains _delta_log directory)
                        Path deltaLogPath = dirPath.resolve("_delta_log");
                        boolean isDeltaTable = Files.exists(deltaLogPath) && Files.isDirectory(deltaLogPath);
                        
                        // Check if this directory contains Parquet files
                        boolean hasParquetFiles = false;
                        if (!isDeltaTable) {
                            try (Stream<Path> files = Files.list(dirPath)) {
                                hasParquetFiles = files.anyMatch(p -> p.toString().endsWith(".parquet"));
                            }
                        }
                        
                        if (isDeltaTable || hasParquetFiles) {
                            String relativePath = rootPath.relativize(dirPath).toString();
                            
                            if (!discoveredTables.contains(relativePath)) {
                                discoveredTables.add(relativePath);
                                
                                // Build location URL
                                String location;
                                if (useFileProtocol) {
                                    location = "file://" + dirPath.toAbsolutePath().toString();
                                } else if (baseUrl != null) {
                                    location = baseUrl + "/" + relativePath;
                                } else {
                                    location = dirPath.toAbsolutePath().toString();
                                }
                                
                                // Extract table info from path
                                TableInfo tableInfo = extractTableInfoFromPath(relativePath, pattern, share.getName());
                                
                                if (tableInfo != null) {
                                    result.discoveredTables++;
                                    String format = isDeltaTable ? "delta" : "parquet";
                                    log.debug("Discovered {} table: {} in schema: {}", 
                                            format, tableInfo.tableName, tableInfo.schemaName);
                                    
                                    DeltaSchema schema = getOrCreateSchema(tableInfo.schemaName, share);
                                    if (schema != null) {
                                        if (tableInfo.isNewSchema) {
                                            result.createdSchemas++;
                                        }
                                        
                                        if (createTableIfNotExists(tableInfo.tableName, location, format, schema)) {
                                            result.createdTables++;
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Error processing directory: {}", dirPath, e);
                    }
                });
        }
        
        log.info("Completed filesystem scan: {} tables discovered", discoveredTables.size());
        return result;
    }
    
    /**
     * Parse discovery pattern into components
     */
    private DiscoveryPattern parseDiscoveryPattern(String patternStr) {
        DiscoveryPattern pattern = new DiscoveryPattern();
        pattern.rawPattern = patternStr;
        
        // Extract placeholders like {bucket}, {schema}, {table}
        Pattern placeholderPattern = Pattern.compile("\\{([^}]+)\\}");
        Matcher matcher = placeholderPattern.matcher(patternStr);
        
        while (matcher.find()) {
            String placeholder = matcher.group(1);
            pattern.placeholders.add(placeholder);
            
            // Set flags based on placeholders
            if (placeholder.equalsIgnoreCase("share")) {
                pattern.hasShare = true;
            } else if (placeholder.equalsIgnoreCase("schema")) {
                pattern.hasSchema = true;
            } else if (placeholder.equalsIgnoreCase("table")) {
                pattern.hasTable = true;
            }
        }
        
        log.debug("Parsed discovery pattern: {} with placeholders: {} (hasShare={}, hasSchema={}, hasTable={})", 
                  patternStr, pattern.placeholders, pattern.hasShare, pattern.hasSchema, pattern.hasTable);
        
        return pattern;
    }
    
    /**
     * Create or get existing schema
     */
    @Transactional
    protected DeltaSchema getOrCreateSchema(String schemaName, DeltaShare share) {
        // Check if schema already exists
        Optional<DeltaSchema> existing = schemaRepository.findAll().stream()
                .filter(s -> s.getName().equals(schemaName) && 
                           s.getShare().getId().equals(share.getId()))
                .findFirst();
        
        if (existing.isPresent()) {
            return existing.get();
        }
        
        // Create new schema
        if (crawlerProperties.isDryRun()) {
            log.info("[DRY-RUN] Would create schema: {} in share: {}", schemaName, share.getName());
            return null;
        }
        
        if (!crawlerProperties.isAutoCreateSchemas()) {
            log.warn("Schema {} does not exist and auto-create is disabled", schemaName);
            return null;
        }
        
        DeltaSchema newSchema = new DeltaSchema();
        newSchema.setName(schemaName);
        newSchema.setDescription("Auto-discovered by table crawler");
        newSchema.setShare(share);
        newSchema.setCreatedAt(LocalDateTime.now());
        newSchema.setUpdatedAt(LocalDateTime.now());
        
        DeltaSchema saved = schemaRepository.save(newSchema);
        log.info("✅ Created schema: {} in share: {}", schemaName, share.getName());
        
        return saved;
    }
    
    /**
     * Create table if it doesn't exist
     */
    @Transactional
    protected boolean createTableIfNotExists(String tableName, String location, String format, DeltaSchema schema) {
        // Check if table already exists
        boolean exists = tableRepository.findAll().stream()
                .anyMatch(t -> t.getName().equals(tableName) && 
                             t.getSchema().getId().equals(schema.getId()));
        
        if (exists) {
            log.debug("Table {} already exists in schema {}", tableName, schema.getName());
            return false;
        }
        
        if (crawlerProperties.isDryRun()) {
            log.info("[DRY-RUN] Would create table: {} in schema: {} with location: {}", 
                     tableName, schema.getName(), location);
            return false;
        }
        
        DeltaTable newTable = new DeltaTable();
        newTable.setName(tableName);
        newTable.setDescription("Auto-discovered by table crawler");
        newTable.setSchema(schema);
        newTable.setLocation(location);
        newTable.setFormat(format);
        newTable.setShareAsView(false);
        newTable.setCreatedAt(LocalDateTime.now());
        newTable.setUpdatedAt(LocalDateTime.now());
        
        // Mark as auto-discovered
        newTable.setDiscoveredAt(LocalDateTime.now());
        newTable.setDiscoveredBy("crawler");
        
        tableRepository.save(newTable);
        log.info("✅ Created table: {} in schema: {} ({})", tableName, schema.getName(), location);
        
        return true;
    }
    
    /**
     * Result of a crawling operation
     */
    private static class CrawlerResult {
        int discoveredTables = 0;
        int createdTables = 0;
        int createdSchemas = 0;
    }
    
    /**
     * Parsed discovery pattern
     */
    private static class DiscoveryPattern {
        String rawPattern;
        List<String> placeholders = new ArrayList<>();
        boolean hasShare = false;
        boolean hasSchema = false;
        boolean hasTable = false;
    }
    
    /**
     * Information extracted from a discovered table path
     */
    private static class TableInfo {
        String shareName;
        String schemaName;
        String tableName;
        boolean isNewSchema = false;
    }
    
    /**
     * Extract table path from a Delta log file path
     * Example: "schema/table/_delta_log/00000.json" -> "schema/table"
     */
    private String extractTablePathFromDeltaLog(String deltaLogPath) {
        int deltaLogIndex = deltaLogPath.indexOf("_delta_log");
        if (deltaLogIndex > 0) {
            String tablePath = deltaLogPath.substring(0, deltaLogIndex);
            // Remove trailing slash
            return tablePath.endsWith("/") ? tablePath.substring(0, tablePath.length() - 1) : tablePath;
        }
        return null;
    }
    
    /**
     * Extract table path from a Parquet file path
     * Example: "schema/table/part-00000.parquet" -> "schema/table"
     */
    private String extractTablePathFromParquet(String parquetPath) {
        // Get the directory containing the parquet file
        int lastSlash = parquetPath.lastIndexOf('/');
        if (lastSlash > 0) {
            return parquetPath.substring(0, lastSlash);
        }
        return null;
    }
    
    /**
     * Extract table info from S3 path using pattern
     */
    private TableInfo extractTableInfo(String fullPath, DiscoveryPattern pattern, String bucket, String shareName) {
        TableInfo info = new TableInfo();
        
        // Remove protocol prefix
        String path = fullPath.replaceFirst("^s3://[^/]+/", "");
        
        // Split path into components
        String[] pathParts = path.split("/");
        
        // Map pattern to path parts based on discovery pattern
        if (pattern.hasShare && pathParts.length >= 3) {
            // Pattern: {bucket}/{share}/{schema}/{table}
            info.shareName = pathParts[0];
            info.schemaName = pathParts[1];
            info.tableName = pathParts[2];
        } else if (pattern.hasSchema && pathParts.length >= 2) {
            // Pattern: {bucket}/{schema}/{table}
            info.shareName = shareName;
            info.schemaName = pathParts[0];
            info.tableName = pathParts[1];
        } else if (pathParts.length >= 1) {
            // Pattern: {bucket}/{table} - use default schema
            info.shareName = shareName;
            info.schemaName = "default";
            info.tableName = pathParts[0];
        } else {
            return null;
        }
        
        return info;
    }
    
    /**
     * Extract table info from relative path using pattern
     */
    private TableInfo extractTableInfoFromPath(String relativePath, DiscoveryPattern pattern, String shareName) {
        TableInfo info = new TableInfo();
        
        // Normalize path separators
        String normalizedPath = relativePath.replace("\\", "/");
        
        // Split path into components
        String[] pathParts = normalizedPath.split("/");
        
        // Map pattern to path parts
        if (pattern.hasShare && pathParts.length >= 3) {
            info.shareName = pathParts[0];
            info.schemaName = pathParts[1];
            info.tableName = pathParts[2];
        } else if (pattern.hasSchema && pathParts.length >= 2) {
            info.shareName = shareName;
            info.schemaName = pathParts[0];
            info.tableName = pathParts[1];
        } else if (pathParts.length >= 1) {
            info.shareName = shareName;
            info.schemaName = "default";
            info.tableName = pathParts[0];
        } else {
            return null;
        }
        
        return info;
    }
}

