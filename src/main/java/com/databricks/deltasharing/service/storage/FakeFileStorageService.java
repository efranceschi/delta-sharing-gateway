package com.databricks.deltasharing.service.storage;

import com.databricks.deltasharing.dto.delta.AddAction;
import com.databricks.deltasharing.dto.delta.FileStatistics;
import com.databricks.deltasharing.dto.protocol.FileResponse;
import com.databricks.deltasharing.model.DeltaTable;
import com.databricks.deltasharing.service.delta.DataSkippingService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Fake implementation of FileStorageService for testing purposes.
 * Generates REAL Parquet files with fake data in a temporary directory.
 * Files are served via /fake-files endpoint.
 */
@Service
@Slf4j
@ConditionalOnProperty(name = "delta.sharing.storage.type", havingValue = "fake", matchIfMissing = true)
public class FakeFileStorageService implements FileStorageService {
    
    @Value("${delta.sharing.storage.fake.url-protocol:http}")
    private String urlProtocol;
    
    @Value("${delta.sharing.storage.fake.base-url:http://localhost:8080}")
    private String baseUrl;
    
    @Value("${server.port:8080}")
    private int serverPort;
    
    @Autowired(required = false)
    private DataSkippingService dataSkippingService;
    
    private File tempDir;
    private final Map<String, File> generatedFiles = new HashMap<>();
    private static final long DEFAULT_RECORDS_PER_FILE = 1000;
    private static final int DEFAULT_FILE_COUNT = 10; // Generate more files to test data skipping
    
    // Partition templates based on common patterns
    private static final String[][] PARTITION_PATTERNS = {
        {"year", "month"},           // Time-based
        {"region", "country"},       // Geographic
        {"category", "subcategory"}, // Hierarchical
        {"status", "type"},          // Categorical
        {"date"},                    // Single date
        {}                           // No partitions
    };
    
    @PostConstruct
    public void init() throws IOException {
        // Create temporary directory for Parquet files
        tempDir = Files.createTempDirectory("delta-sharing-fake-").toFile();
        log.info("Initialized FakeFileStorageService:");
        log.info("  - Temp directory: {}", tempDir.getAbsolutePath());
        log.info("  - URL protocol: {}", urlProtocol);
        log.info("  - Base URL: {}", baseUrl);
        log.info("  - File endpoint: {}/fake-files/", baseUrl);
    }
    
    @PreDestroy
    public void cleanup() {
        // Clean up temporary files
        if (tempDir != null && tempDir.exists()) {
            deleteDirectory(tempDir);
            log.info("Cleaned up temporary directory: {}", tempDir.getAbsolutePath());
        }
    }
    
    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }
    
    @Override
    public List<FileResponse> getTableFiles(DeltaTable table, Long version, 
                                             List<String> predicateHints, Integer limitHint) {
        log.debug("Generating fake files for table: {} (location: {})", 
                  table.getName(), table.getLocation());
        
        // Generate more files to test data skipping
        int totalFileCount = DEFAULT_FILE_COUNT;
        
        long currentVersion = version != null ? version : 0L;
        long currentTimestamp = System.currentTimeMillis();
        
        // Get schema for this table
        String schemaString = generateFakeSchema(table.getName(), table.getFormat());
        
        // Select partition pattern based on table name hash
        String[] partitionColumns = selectPartitionPattern(table.getName());
        
        // Use timestamp to ensure unique file names across requests
        long timestamp = System.currentTimeMillis();
        
        // Step 1: Generate all files as AddAction objects (simulating Delta log)
        List<AddAction> allAddActions = new ArrayList<>();
        
        for (int i = 0; i < totalFileCount; i++) {
            String fileId = String.format("%s-file-%d-%d", table.getName(), i, timestamp);
            String fileName = String.format("part-%05d-%s.parquet", i, fileId);
            
            // Create dynamic partition values based on selected pattern
            Map<String, String> partitionValues = generatePartitionValues(partitionColumns, i);
            
            try {
                // Generate actual Parquet file
                File parquetFile = new File(tempDir, fileName);
                Map<String, Object> stats = ParquetFileGenerator.generateParquetFile(
                    schemaString,
                    parquetFile,
                    DEFAULT_RECORDS_PER_FILE,
                    partitionValues
                );
                
                // Store file reference
                generatedFiles.put(fileName, parquetFile);
                
                // Create FileStatistics from generated stats
                FileStatistics fileStats = FileStatistics.builder()
                        .numRecords((Long) stats.get("numRecords"))
                        .minValues((Map<String, Object>) stats.get("minValues"))
                        .maxValues((Map<String, Object>) stats.get("maxValues"))
                        .nullCount((Map<String, Long>) stats.get("nullCount"))
                        .build();
                
                // Create AddAction (simulating Delta log entry)
                AddAction addAction = AddAction.builder()
                        .path(fileName)
                        .size(parquetFile.length())
                        .partitionValues(partitionValues)
                        .parsedStats(fileStats)
                        .modificationTime(currentTimestamp - (i * 1000))
                        .dataChange(true)
                        .build();
                
                allAddActions.add(addAction);
                
                log.debug("Generated fake file: {} with partitions: {}", fileName, partitionValues);
                
            } catch (IOException e) {
                log.error("Failed to generate Parquet file: {}", fileName, e);
                // Continue with other files
            }
        }
        
        log.info("Generated {} fake files for table: {}", allAddActions.size(), table.getName());
        
        // Step 2: Apply data skipping with predicates (if available)
        List<AddAction> filteredActions = allAddActions;
        
        if (dataSkippingService != null && predicateHints != null && !predicateHints.isEmpty()) {
            filteredActions = dataSkippingService.applyDataSkipping(allAddActions, predicateHints);
            log.info("Data skipping: {} -> {} files", allAddActions.size(), filteredActions.size());
        } else {
            log.debug("No data skipping applied (no predicates or service unavailable)");
        }
        
        // Step 3: Apply limitHint
        int limit = limitHint != null && limitHint > 0 ? limitHint : Integer.MAX_VALUE;
        List<AddAction> limitedActions = filteredActions.stream()
                .limit(limit)
                .collect(Collectors.toList());
        
        // Step 4: Convert AddAction to FileResponse
        List<FileResponse> files = limitedActions.stream()
                .map(addAction -> convertAddActionToFileResponse(addAction, currentVersion, currentTimestamp))
                .filter(response -> response != null)
                .collect(Collectors.toList());
        
        log.info("Returning {} files after data skipping and limit (from {} total)", 
                 files.size(), allAddActions.size());
        
        return files;
    }
    
    /**
     * Convert AddAction to FileResponse
     */
    private FileResponse convertAddActionToFileResponse(AddAction addAction, long version, long currentTimestamp) {
        try {
            String fileName = addAction.getPath();
            String fileUrl = String.format("%s/fake-files/%s", baseUrl, fileName);
            
            // Convert FileStatistics to Map
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
                    .id(fileName.replace(".parquet", ""))
                    .partitionValues(addAction.getPartitionValues())
                    .size(addAction.getSize())
                    .stats(stats)
                    .version(version)
                    .timestamp(addAction.getModificationTime())
                    .expirationTimestamp(currentTimestamp + (3600 * 1000)) // 1 hour
                    .build();
                    
        } catch (Exception e) {
            log.error("Failed to convert AddAction to FileResponse: {}", addAction.getPath(), e);
            return null;
        }
    }
    
    /**
     * Get a generated Parquet file by name
     */
    public File getGeneratedFile(String fileName) {
        return generatedFiles.get(fileName);
    }
    
    /**
     * Select partition pattern based on table name
     */
    private String[] selectPartitionPattern(String tableName) {
        // Use table name hash to consistently select a partition pattern
        int patternIndex = Math.abs(tableName.hashCode()) % PARTITION_PATTERNS.length;
        return PARTITION_PATTERNS[patternIndex];
    }
    
    /**
     * Generate partition values based on partition columns
     */
    private Map<String, String> generatePartitionValues(String[] partitionColumns, int fileIndex) {
        Map<String, String> partitionValues = new HashMap<>();
        
        for (String column : partitionColumns) {
            String value = switch (column) {
                case "year" -> String.valueOf(2020 + (fileIndex % 5));
                case "month" -> String.format("%02d", (fileIndex % 12) + 1);
                case "date" -> String.format("2024-%02d-%02d", (fileIndex % 12) + 1, (fileIndex % 28) + 1);
                case "region" -> new String[]{"us-east", "us-west", "eu-central", "ap-south"}[fileIndex % 4];
                case "country" -> new String[]{"US", "UK", "DE", "IN", "BR", "CN"}[fileIndex % 6];
                case "category" -> new String[]{"electronics", "books", "clothing", "home", "toys"}[fileIndex % 5];
                case "subcategory" -> new String[]{"laptops", "phones", "tablets", "accessories"}[fileIndex % 4];
                case "status" -> new String[]{"active", "inactive", "pending", "completed"}[fileIndex % 4];
                case "type" -> new String[]{"A", "B", "C", "D"}[fileIndex % 4];
                default -> String.format("value_%d", fileIndex);
            };
            partitionValues.put(column, value);
        }
        
        return partitionValues;
    }
    
    @Override
    public String getStorageType() {
        return "fake";
    }
    
    @Override
    public boolean isAvailable() {
        return tempDir != null && tempDir.exists();
    }
    
    /**
     * Generate a fake schema string based on table name and format.
     * Creates a realistic Delta/Parquet schema with varied column types and metadata.
     */
    public static String generateFakeSchema(String tableName, String format) {
        // Select schema complexity based on table name
        boolean isFactTable = tableName.startsWith("fact_");
        boolean isDimTable = tableName.startsWith("dim_");
        boolean isAggTable = tableName.startsWith("agg_");
        
        List<String> fields = new ArrayList<>();
        
        // Always include ID with metadata
        fields.add(createField("id", "long", false, "Primary key", null, null));
        
        // Add fields based on table type
        if (isFactTable) {
            // Fact tables: metrics and foreign keys
            fields.add(createField("customer_id", "long", true, "Foreign key to customer dimension", null, null));
            fields.add(createField("product_id", "long", true, "Foreign key to product dimension", null, null));
            fields.add(createField("quantity", "integer", true, "Quantity of items", 0, 10000));
            fields.add(createField("amount", "decimal(10,2)", true, "Transaction amount in USD", null, null));
            fields.add(createField("timestamp", "timestamp", false, "Transaction timestamp", null, null));
        } else if (isDimTable) {
            // Dimension tables: descriptive attributes
            fields.add(createField("name", "string", true, "Display name", 1, 255));
            fields.add(createField("description", "string", true, "Detailed description", null, 1000));
            fields.add(createField("category", "string", true, "Category classification", null, null));
            fields.add(createField("status", "string", true, "Current status", null, null));
            fields.add(createField("created_at", "timestamp", false, "Record creation timestamp", null, null));
            fields.add(createField("updated_at", "timestamp", true, "Last update timestamp", null, null));
        } else if (isAggTable) {
            // Aggregate tables: summary metrics
            fields.add(createField("period", "string", false, "Aggregation period (daily/monthly/yearly)", null, null));
            fields.add(createField("group_by", "string", true, "Grouping dimension", null, null));
            fields.add(createField("total_count", "long", false, "Total record count", 0, null));
            fields.add(createField("sum_amount", "decimal(18,2)", true, "Sum of amounts", null, null));
            fields.add(createField("avg_amount", "decimal(10,2)", true, "Average amount", null, null));
            fields.add(createField("min_amount", "decimal(10,2)", true, "Minimum amount", null, null));
            fields.add(createField("max_amount", "decimal(10,2)", true, "Maximum amount", null, null));
        } else {
            // Generic table: mixed columns
            fields.add(createField("data", "string", true, "Data payload", null, 5000));
            fields.add(createField("value", "double", true, "Numeric value", null, null));
            fields.add(createField("flag", "boolean", true, "Boolean flag", null, null));
            fields.add(createField("timestamp", "timestamp", true, "Event timestamp", null, null));
        }
        
        return String.format("{\"type\":\"struct\",\"fields\":[%s]}", String.join(",", fields));
    }
    
    /**
     * Create a field with rich metadata
     */
    private static String createField(String name, String type, boolean nullable, 
                                     String comment, Integer minValue, Integer maxValue) {
        StringBuilder metadata = new StringBuilder("{");
        
        List<String> metadataItems = new ArrayList<>();
        
        // Add comment
        if (comment != null && !comment.isEmpty()) {
            metadataItems.add(String.format("\"comment\":\"%s\"", comment));
        }
        
        // Add constraints for numeric types
        if (minValue != null) {
            metadataItems.add(String.format("\"minValue\":%d", minValue));
        }
        if (maxValue != null) {
            metadataItems.add(String.format("\"maxValue\":%d", maxValue));
        }
        
        // Add field-specific metadata
        if (name.equals("id")) {
            metadataItems.add("\"isPrimaryKey\":true");
        } else if (name.endsWith("_id")) {
            metadataItems.add("\"isForeignKey\":true");
        }
        
        if (name.contains("timestamp") || name.contains("_at")) {
            metadataItems.add("\"isTimestamp\":true");
        }
        
        if (type.startsWith("decimal")) {
            metadataItems.add("\"isCurrency\":true");
            metadataItems.add("\"currency\":\"USD\"");
        }
        
        if (name.equals("status") || name.equals("category")) {
            metadataItems.add("\"isCategorical\":true");
        }
        
        metadata.append(String.join(",", metadataItems));
        metadata.append("}");
        
        return String.format(
            "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":%s,\"metadata\":%s}",
            name, type, nullable, metadata
        );
    }
    
    /**
     * Get partition columns based on table name.
     * Returns a list of column names that should be used for partitioning.
     */
    public String[] getPartitionColumns(String tableName) {
        return selectPartitionPattern(tableName);
    }
}
