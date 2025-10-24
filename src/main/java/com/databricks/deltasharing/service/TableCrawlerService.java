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
import org.springframework.scheduling.annotation.Async;
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
     * Trigger manual crawler execution asynchronously
     */
    @Async
    public void triggerManualCrawl() {
        log.info("🚀 Manual crawler trigger - Starting asynchronous execution");
        crawlTables();
    }
    
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
            log.debug("🔍 Fetching all shares from repository...");
            List<DeltaShare> allShares = shareRepository.findAll();
            log.debug("📊 Total shares in database: {}", allShares.size());
            
            List<DeltaShare> activeShares = allShares.stream()
                    .filter(DeltaShare::getActive)
                    .toList();
            
            log.info("✅ Found {} active share(s) to scan (out of {} total)", activeShares.size(), allShares.size());
            
            if (activeShares.isEmpty()) {
                log.warn("⚠️ No active shares found. Crawler has nothing to scan.");
                log.warn("   Create shares in the web interface at /admin/shares");
            }
            
            // Log details of each share
            for (DeltaShare share : activeShares) {
                log.debug("   → Share: {} (ID: {}, Active: {})", share.getName(), share.getId(), share.getActive());
            }
            
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
            log.debug("════════════════════════════════════════════════════════════");
            log.debug("🔍 Scanning storage for share: {}", share.getName());
            log.debug("════════════════════════════════════════════════════════════");
            
            // Parse discovery pattern
            DiscoveryPattern pattern = parseDiscoveryPattern(crawlerProperties.getDiscoveryPattern());
            log.debug("📋 Discovery pattern: {}", pattern.rawPattern);
            log.debug("   → Has share placeholder: {}", pattern.hasShare);
            log.debug("   → Has schema placeholder: {}", pattern.hasSchema);
            log.debug("   → Has table placeholder: {}", pattern.hasTable);
            
            // Based on storage type, scan for tables
            String storageType = storageConfig.getType().toUpperCase();
            log.debug("🗄️  Storage type: {}", storageType);
            
            switch (storageType) {
                case "MINIO":
                    log.debug("   → Scanning MinIO/S3 storage...");
                    result = scanMinIOStorage(share, pattern);
                    break;
                    
                case "HTTP":
                    log.debug("   → Scanning HTTP storage...");
                    result = scanHttpStorage(share, pattern);
                    break;
                    
                case "FILESYSTEM":
                    log.debug("   → Scanning Filesystem storage...");
                    result = scanFilesystemStorage(share, pattern);
                    break;
                    
                case "FAKE":
                    log.debug("⏭️  Skipping FAKE storage type (no real data to discover)");
                    log.debug("   FAKE storage is for synthetic data generation only.");
                    break;
                    
                default:
                    log.warn("❌ Unsupported storage type for crawler: {}", storageType);
                    log.warn("   Supported types: MINIO, HTTP, FILESYSTEM, FAKE");
            }
            
            log.debug("📊 Scan results for share '{}':", share.getName());
            log.debug("   → Discovered: {} tables", result.discoveredTables);
            log.debug("   → Created: {} tables", result.createdTables);
            log.debug("   → Created: {} schemas", result.createdSchemas);
            
        } catch (Exception e) {
            log.error("❌ Error scanning storage for share: {}", share.getName(), e);
            log.error("   Exception type: {}", e.getClass().getName());
            log.error("   Message: {}", e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Scan MinIO/S3 storage for tables
     */
    private CrawlerResult scanMinIOStorage(DeltaShare share, DiscoveryPattern pattern) {
        CrawlerResult result = new CrawlerResult();
        
        try {
            log.debug("┌─ MinIO Storage Scan");
            log.debug("│  Share: {}", share.getName());
            
            // Check MinIO configuration
            if (storageConfig.getMinio() == null) {
                log.error("│  ❌ MinIO configuration is null!");
                log.error("│     Configure MinIO in application.yml under delta.sharing.storage.minio");
                return result;
            }
            
            String endpoint = storageConfig.getMinio().getEndpoint();
            String accessKey = storageConfig.getMinio().getAccessKey();
            
            // Use share name as bucket name
            String bucket = share.getName();
            
            log.debug("│  Endpoint: {}", endpoint);
            log.debug("│  Bucket (from share name): {}", bucket);
            log.debug("│  Access Key: {}", accessKey != null ? accessKey.substring(0, Math.min(4, accessKey.length())) + "***" : "null");
            
            if (endpoint == null || accessKey == null) {
                log.error("│  ❌ MinIO configuration is incomplete!");
                log.error("│     endpoint={}, accessKey={}", endpoint, accessKey);
                return result;
            }
            
            if (bucket == null || bucket.trim().isEmpty()) {
                log.error("│  ❌ Share name is empty - cannot determine bucket name!");
                return result;
            }
            
            // Build MinIO client from configuration
            log.debug("│  Building MinIO client...");
            MinioClient minioClient = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, storageConfig.getMinio().getSecretKey())
                    .build();
            log.debug("│  ✅ MinIO client created successfully");
            
            // List all objects in bucket
            log.debug("│  Listing objects in bucket '{}'...", bucket);
            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(bucket)
                            .recursive(true)
                            .build()
            );
            log.debug("│  ✅ Object listing started");
            
            // Track discovered paths
            Set<String> discoveredTables = new HashSet<>();
            int totalObjects = 0;
            int deltaLogObjects = 0;
            int parquetObjects = 0;
            
            for (Result<Item> itemResult : results) {
                totalObjects++;
                String objectPath = null;
                try {
                    Item item = itemResult.get();
                    objectPath = item.objectName();
                    
                    // Log every 100 objects to show progress
                    if (totalObjects % 100 == 0) {
                        log.debug("│  Progress: Scanned {} objects... (Delta logs: {}, Parquet: {})", 
                                  totalObjects, deltaLogObjects, parquetObjects);
                    }
                    
                    // Check if this is a Delta table (_delta_log directory)
                    if (objectPath.contains("_delta_log/")) {
                        deltaLogObjects++;
                        log.trace("│    → Found Delta log: {}", objectPath);
                        
                        String tablePath = extractTablePathFromDeltaLog(objectPath);
                        if (tablePath != null && !discoveredTables.contains(tablePath)) {
                            discoveredTables.add(tablePath);
                            
                            // Build full S3 path
                            String fullPath = String.format("s3://%s/%s", bucket, tablePath);
                            log.debug("│  📦 Discovered Delta table path: {}", fullPath);
                            
                            // Extract schema and table from path using pattern
                            TableInfo tableInfo = extractTableInfo(fullPath, pattern, bucket, share.getName());
                            
                            if (tableInfo != null) {
                                result.discoveredTables++;
                                log.info("│  ✅ Discovered Delta table: {} in schema: {}", tableInfo.tableName, tableInfo.schemaName);
                                
                                // Create schema if needed
                                DeltaSchema schema = getOrCreateSchema(tableInfo.schemaName, share);
                                if (schema != null) {
                                    if (tableInfo.isNewSchema) {
                                        result.createdSchemas++;
                                        log.info("│     ➕ Created new schema: {}", tableInfo.schemaName);
                                    }
                                    
                                    // Create table if not exists
                                    if (createTableIfNotExists(tableInfo.tableName, fullPath, "delta", schema)) {
                                        result.createdTables++;
                                        log.info("│     ➕ Created new table: {}", tableInfo.tableName);
                                    } else {
                                        log.debug("│     ⏭️  Table already exists: {}", tableInfo.tableName);
                                    }
                                }
                            } else {
                                log.debug("│     ⏭️  Could not extract table info from: {}", fullPath);
                            }
                        }
                    }
                    // Check for Parquet files
                    else if (objectPath.endsWith(".parquet") && !objectPath.contains("_delta_log/")) {
                        parquetObjects++;
                        log.trace("│    → Found Parquet file: {}", objectPath);
                        
                        String tablePath = extractTablePathFromParquet(objectPath);
                        if (tablePath != null && !discoveredTables.contains(tablePath)) {
                            discoveredTables.add(tablePath);
                            
                            String fullPath = String.format("s3://%s/%s", bucket, tablePath);
                            log.debug("│  📦 Discovered Parquet table path: {}", fullPath);
                            
                            TableInfo tableInfo = extractTableInfo(fullPath, pattern, bucket, share.getName());
                            
                            if (tableInfo != null) {
                                result.discoveredTables++;
                                log.info("│  ✅ Discovered Parquet table: {} in schema: {}", tableInfo.tableName, tableInfo.schemaName);
                                
                                DeltaSchema schema = getOrCreateSchema(tableInfo.schemaName, share);
                                if (schema != null) {
                                    if (tableInfo.isNewSchema) {
                                        result.createdSchemas++;
                                        log.info("│     ➕ Created new schema: {}", tableInfo.schemaName);
                                    }
                                    
                                    if (createTableIfNotExists(tableInfo.tableName, fullPath, "parquet", schema)) {
                                        result.createdTables++;
                                        log.info("│     ➕ Created new table: {}", tableInfo.tableName);
                                    } else {
                                        log.debug("│     ⏭️  Table already exists: {}", tableInfo.tableName);
                                    }
                                }
                            } else {
                                log.debug("│     ⏭️  Could not extract table info from: {}", fullPath);
                            }
                        }
                    }
                    
                } catch (Exception e) {
                    log.warn("│  ⚠️  Error processing object: {} - {}", objectPath, e.getMessage());
                    log.debug("│     Exception details:", e);
                }
            }
            
            log.info("└─ Completed MinIO scan");
            log.info("   Total objects scanned: {}", totalObjects);
            log.info("   Delta log objects: {}", deltaLogObjects);
            log.info("   Parquet files: {}", parquetObjects);
            log.info("   Unique tables discovered: {}", discoveredTables.size());
            
            if (totalObjects == 0) {
                log.warn("   ⚠️  No objects found in bucket '{}'!", bucket);
                log.warn("      Is the bucket empty or are credentials correct?");
            }
            
        } catch (Exception e) {
            log.error("└─ ❌ Error scanning MinIO storage for share: {}", share.getName());
            log.error("   Exception: {}", e.getClass().getName());
            log.error("   Message: {}", e.getMessage());
            log.debug("   Full stack trace:", e);
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
     * Note: bucket parameter is the share name (bucket = share in S3/MinIO)
     */
    private TableInfo extractTableInfo(String fullPath, DiscoveryPattern pattern, String bucket, String shareName) {
        TableInfo info = new TableInfo();
        
        // Remove protocol prefix (s3://bucket/)
        // Since bucket = share name, this removes s3://share-name/
        String path = fullPath.replaceFirst("^s3://[^/]+/", "");
        
        // Split path into components
        String[] pathParts = path.split("/");
        
        // Map pattern to path parts based on discovery pattern
        if (pattern.hasShare && pathParts.length >= 3) {
            // Pattern: s3://{share}/{extra-level}/{schema}/{table}
            // Not commonly used since share = bucket
            info.shareName = pathParts[0];
            info.schemaName = pathParts[1];
            info.tableName = pathParts[2];
        } else if (pattern.hasSchema && pathParts.length >= 2) {
            // Pattern: s3://{share}/{schema}/{table}
            // Most common: share name is bucket, then schema/table inside
            info.shareName = shareName;
            info.schemaName = pathParts[0];
            info.tableName = pathParts[1];
        } else if (pathParts.length >= 1) {
            // Pattern: s3://{share}/{table} - use default schema
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

