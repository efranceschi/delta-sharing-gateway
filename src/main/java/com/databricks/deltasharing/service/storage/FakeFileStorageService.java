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
import org.springframework.cache.annotation.Cacheable;
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
     * Get the schema for a table (interface implementation)
     * Cached to avoid repeated schema generation
     */
    @Override
    @Cacheable(value = "tableSchemas", key = "#tableName + '_' + #format")
    public String getTableSchema(String tableName, String format) {
        log.debug("Generating (uncached) schema for table: {} with format: {}", tableName, format);
        return generateFakeSchema(tableName, format);
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
     * Get partition columns based on table name (interface implementation).
     * Returns a list of column names that should be used for partitioning.
     */
    @Override
    @Cacheable(value = "partitionColumns", key = "#tableName")
    public String[] getPartitionColumns(String tableName) {
        log.debug("Generating (uncached) partition columns for table: {}", tableName);
        return selectPartitionPattern(tableName);
    }
    
    /**
     * Inner static class for generating Parquet files with fake data.
     * Converts Delta schemas to Avro schemas and writes Parquet files.
     */
    @Slf4j
    static class ParquetFileGenerator {

        private static final com.fasterxml.jackson.databind.ObjectMapper objectMapper = 
            new com.fasterxml.jackson.databind.ObjectMapper();
        private static final Random random = new Random();

        /**
         * Generate a Parquet file with fake data based on the provided schema
         *
         * @param schemaJson Delta schema in JSON format
         * @param outputFile Output Parquet file
         * @param numRecords Number of records to generate
         * @param partitionValues Partition values for this file
         * @return Statistics about the generated file
         */
        static Map<String, Object> generateParquetFile(
                String schemaJson,
                File outputFile,
                long numRecords,
                Map<String, String> partitionValues) throws IOException {

            log.debug("Generating Parquet file: {} with {} records", outputFile.getName(), numRecords);

            // Parse Delta schema to Avro schema
            org.apache.avro.Schema avroSchema = deltaSchemaToAvroSchema(schemaJson);

            // Create Parquet writer
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputFile.toURI());
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            
            try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer = 
                    org.apache.parquet.avro.AvroParquetWriter
                    .<org.apache.avro.generic.GenericRecord>builder(path)
                    .withSchema(avroSchema)
                    .withConf(conf)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
                    .build()) {

                // Statistics tracking
                Map<String, Object> minValues = new HashMap<>();
                Map<String, Object> maxValues = new HashMap<>();
                Map<String, Integer> nullCount = new HashMap<>();

                // Generate records
                for (long i = 0; i < numRecords; i++) {
                    org.apache.avro.generic.GenericRecord record = generateRecord(avroSchema, i, partitionValues);
                    writer.write(record);

                    // Update statistics
                    updateStatistics(record, avroSchema, minValues, maxValues, nullCount);
                }

                log.info("Generated Parquet file: {} ({} records, {} bytes)",
                        outputFile.getName(), numRecords, outputFile.length());

                // Return statistics
                Map<String, Object> stats = new HashMap<>();
                stats.put("numRecords", numRecords);
                stats.put("minValues", minValues);
                stats.put("maxValues", maxValues);
                stats.put("nullCount", nullCount);
                return stats;
            }
        }

        /**
         * Convert Delta schema JSON to Avro Schema
         */
        private static org.apache.avro.Schema deltaSchemaToAvroSchema(String schemaJson) throws IOException {
            com.fasterxml.jackson.databind.JsonNode deltaSchema = objectMapper.readTree(schemaJson);
            
            // Build Avro schema
            List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
            com.fasterxml.jackson.databind.JsonNode fieldsNode = deltaSchema.get("fields");
            
            if (fieldsNode != null && fieldsNode.isArray()) {
                for (com.fasterxml.jackson.databind.JsonNode field : fieldsNode) {
                    String name = field.get("name").asText();
                    String type = field.get("type").asText();
                    boolean nullable = field.get("nullable").asBoolean(true);
                    
                    org.apache.avro.Schema fieldSchema = deltaTypeToAvroType(type, nullable);
                    // No default value for Avro fields
                    fields.add(new org.apache.avro.Schema.Field(name, fieldSchema, null, (Object) null));
                }
            }
            
            return org.apache.avro.Schema.createRecord("DeltaTable", "Generated from Delta schema", 
                "com.databricks.delta", false, fields);
        }

        /**
         * Convert Delta type to Avro type
         */
        private static org.apache.avro.Schema deltaTypeToAvroType(String deltaType, boolean nullable) {
            org.apache.avro.Schema baseSchema;
            
            switch (deltaType.toLowerCase()) {
                case "long":
                case "bigint":
                    baseSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
                    break;
                case "integer":
                case "int":
                    baseSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
                    break;
                case "double":
                    baseSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE);
                    break;
                case "float":
                    baseSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT);
                    break;
                case "boolean":
                    baseSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN);
                    break;
                case "string":
                    baseSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
                    break;
                case "timestamp":
                    // Timestamp as long (milliseconds since epoch)
                    baseSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
                    break;
                default:
                    if (deltaType.startsWith("decimal")) {
                        // Decimal as double for simplicity
                        baseSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE);
                    } else {
                        // Default to string
                        baseSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
                    }
            }
            
            return nullable ? org.apache.avro.Schema.createUnion(
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), baseSchema) : baseSchema;
        }

        /**
         * Generate a single record with fake data
         */
        private static org.apache.avro.generic.GenericRecord generateRecord(
                org.apache.avro.Schema schema, long recordIndex, Map<String, String> partitionValues) {
            org.apache.avro.generic.GenericRecord record = new org.apache.avro.generic.GenericData.Record(schema);
            
            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                org.apache.avro.Schema fieldSchema = field.schema();
                
                // Check if this is a partition column
                if (partitionValues.containsKey(fieldName)) {
                    Object value = convertPartitionValue(partitionValues.get(fieldName), fieldSchema);
                    record.put(fieldName, value);
                    continue;
                }
                
                // Generate fake value based on field name and type
                Object value = generateFieldValue(fieldName, fieldSchema, recordIndex);
                record.put(fieldName, value);
            }
            
            return record;
        }

        /**
         * Convert partition value string to appropriate type
         */
        private static Object convertPartitionValue(String value, org.apache.avro.Schema schema) {
            org.apache.avro.Schema.Type type = schema.getType();
            
            if (type == org.apache.avro.Schema.Type.UNION) {
                // Get non-null type from union
                for (org.apache.avro.Schema s : schema.getTypes()) {
                    if (s.getType() != org.apache.avro.Schema.Type.NULL) {
                        return convertPartitionValue(value, s);
                    }
                }
            }
            
            try {
                switch (type) {
                    case LONG:
                        return Long.parseLong(value);
                    case INT:
                        return Integer.parseInt(value);
                    case DOUBLE:
                        return Double.parseDouble(value);
                    case FLOAT:
                        return Float.parseFloat(value);
                    case BOOLEAN:
                        return Boolean.parseBoolean(value);
                    case STRING:
                    default:
                        return value;
                }
            } catch (NumberFormatException e) {
                return value; // Return as string if conversion fails
            }
        }

        /**
         * Generate fake value for a field
         */
        private static Object generateFieldValue(String fieldName, org.apache.avro.Schema schema, long recordIndex) {
            org.apache.avro.Schema.Type type = schema.getType();
            
            // Handle union types (nullable fields)
            if (type == org.apache.avro.Schema.Type.UNION) {
                // 10% chance of null for nullable fields
                if (random.nextDouble() < 0.1) {
                    return null;
                }
                // Get non-null type
                for (org.apache.avro.Schema s : schema.getTypes()) {
                    if (s.getType() != org.apache.avro.Schema.Type.NULL) {
                        return generateFieldValue(fieldName, s, recordIndex);
                    }
                }
            }
            
            // Generate value based on field name patterns
            if (fieldName.equals("id")) {
                return recordIndex;
            } else if (fieldName.contains("timestamp") || fieldName.contains("_at")) {
                return System.currentTimeMillis() - (random.nextInt(86400000)); // Within last 24 hours
            } else if (fieldName.contains("amount") || fieldName.contains("price") || fieldName.contains("value")) {
                return type == org.apache.avro.Schema.Type.LONG ? 
                       random.nextLong(1000) + 1 : 
                       Math.round(random.nextDouble() * 1000 * 100.0) / 100.0;
            } else if (fieldName.contains("count") || fieldName.contains("quantity")) {
                return type == org.apache.avro.Schema.Type.LONG ? random.nextLong(100) + 1 : random.nextInt(100) + 1;
            } else if (fieldName.contains("name")) {
                return "Name_" + recordIndex;
            } else if (fieldName.contains("status")) {
                String[] statuses = {"active", "inactive", "pending", "completed"};
                return statuses[random.nextInt(statuses.length)];
            } else if (fieldName.contains("category")) {
                String[] categories = {"A", "B", "C", "D"};
                return categories[random.nextInt(categories.length)];
            } else if (fieldName.contains("description")) {
                return "Description for record " + recordIndex;
            } else if (fieldName.contains("data")) {
                return "Data_" + recordIndex + "_" + UUID.randomUUID().toString().substring(0, 8);
            }
            
            // Generate based on type
            switch (type) {
                case LONG:
                    return recordIndex * 1000 + random.nextInt(1000);
                case INT:
                    return (int) (recordIndex % 1000000);
                case DOUBLE:
                    return recordIndex + random.nextDouble();
                case FLOAT:
                    return (float) (recordIndex + random.nextFloat());
                case BOOLEAN:
                    return random.nextBoolean();
                case STRING:
                default:
                    return "value_" + recordIndex;
            }
        }

        /**
         * Update statistics for a record
         */
        private static void updateStatistics(
                org.apache.avro.generic.GenericRecord record,
                org.apache.avro.Schema schema,
                Map<String, Object> minValues,
                Map<String, Object> maxValues,
                Map<String, Integer> nullCount) {
            
            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                Object value = record.get(fieldName);
                
                if (value == null) {
                    nullCount.put(fieldName, nullCount.getOrDefault(fieldName, 0) + 1);
                    continue;
                }
                
                nullCount.putIfAbsent(fieldName, 0);
                
                // Update min/max for comparable types
                if (value instanceof Comparable) {
                    if (!minValues.containsKey(fieldName) || 
                        ((Comparable) value).compareTo(minValues.get(fieldName)) < 0) {
                        minValues.put(fieldName, value);
                    }
                    if (!maxValues.containsKey(fieldName) || 
                        ((Comparable) value).compareTo(maxValues.get(fieldName)) > 0) {
                        maxValues.put(fieldName, value);
                    }
                }
            }
        }
    }
}
