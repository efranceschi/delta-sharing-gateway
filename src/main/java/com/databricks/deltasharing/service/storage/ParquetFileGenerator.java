package com.databricks.deltasharing.service.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Generates realistic Parquet files with fake data based on Delta table schemas.
 * Used by FakeFileStorageService to create actual Parquet files for testing.
 */
@Slf4j
public class ParquetFileGenerator {

    private static final ObjectMapper objectMapper = new ObjectMapper();
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
    public static Map<String, Object> generateParquetFile(
            String schemaJson,
            File outputFile,
            long numRecords,
            Map<String, String> partitionValues) throws IOException {

        log.debug("Generating Parquet file: {} with {} records", outputFile.getName(), numRecords);

        // Parse Delta schema to Avro schema
        Schema avroSchema = deltaSchemaToAvroSchema(schemaJson);

        // Create Parquet writer
        Path path = new Path(outputFile.toURI());
        Configuration conf = new Configuration();
        
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(avroSchema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            // Statistics tracking
            Map<String, Object> minValues = new HashMap<>();
            Map<String, Object> maxValues = new HashMap<>();
            Map<String, Integer> nullCount = new HashMap<>();

            // Generate records
            for (long i = 0; i < numRecords; i++) {
                GenericRecord record = generateRecord(avroSchema, i, partitionValues);
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
    private static Schema deltaSchemaToAvroSchema(String schemaJson) throws IOException {
        JsonNode deltaSchema = objectMapper.readTree(schemaJson);
        
        // Build Avro schema
        List<Schema.Field> fields = new ArrayList<>();
        JsonNode fieldsNode = deltaSchema.get("fields");
        
        if (fieldsNode != null && fieldsNode.isArray()) {
            for (JsonNode field : fieldsNode) {
                String name = field.get("name").asText();
                String type = field.get("type").asText();
                boolean nullable = field.get("nullable").asBoolean(true);
                
                Schema fieldSchema = deltaTypeToAvroType(type, nullable);
                // No default value for Avro fields
                fields.add(new Schema.Field(name, fieldSchema, null, (Object) null));
            }
        }
        
        return Schema.createRecord("DeltaTable", "Generated from Delta schema", "com.databricks.delta", false, fields);
    }

    /**
     * Convert Delta type to Avro type
     */
    private static Schema deltaTypeToAvroType(String deltaType, boolean nullable) {
        Schema baseSchema;
        
        switch (deltaType.toLowerCase()) {
            case "long":
            case "bigint":
                baseSchema = Schema.create(Schema.Type.LONG);
                break;
            case "integer":
            case "int":
                baseSchema = Schema.create(Schema.Type.INT);
                break;
            case "double":
                baseSchema = Schema.create(Schema.Type.DOUBLE);
                break;
            case "float":
                baseSchema = Schema.create(Schema.Type.FLOAT);
                break;
            case "boolean":
                baseSchema = Schema.create(Schema.Type.BOOLEAN);
                break;
            case "string":
                baseSchema = Schema.create(Schema.Type.STRING);
                break;
            case "timestamp":
                // Timestamp as long (milliseconds since epoch)
                baseSchema = Schema.create(Schema.Type.LONG);
                break;
            default:
                if (deltaType.startsWith("decimal")) {
                    // Decimal as double for simplicity
                    baseSchema = Schema.create(Schema.Type.DOUBLE);
                } else {
                    // Default to string
                    baseSchema = Schema.create(Schema.Type.STRING);
                }
        }
        
        return nullable ? Schema.createUnion(Schema.create(Schema.Type.NULL), baseSchema) : baseSchema;
    }

    /**
     * Generate a single record with fake data
     */
    private static GenericRecord generateRecord(Schema schema, long recordIndex, Map<String, String> partitionValues) {
        GenericRecord record = new GenericData.Record(schema);
        
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            
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
    private static Object convertPartitionValue(String value, Schema schema) {
        Schema.Type type = schema.getType();
        
        if (type == Schema.Type.UNION) {
            // Get non-null type from union
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
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
    private static Object generateFieldValue(String fieldName, Schema schema, long recordIndex) {
        Schema.Type type = schema.getType();
        
        // Handle union types (nullable fields)
        if (type == Schema.Type.UNION) {
            // 10% chance of null for nullable fields
            if (random.nextDouble() < 0.1) {
                return null;
            }
            // Get non-null type
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
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
            return type == Schema.Type.LONG ? 
                   random.nextLong(1000) + 1 : 
                   Math.round(random.nextDouble() * 1000 * 100.0) / 100.0;
        } else if (fieldName.contains("count") || fieldName.contains("quantity")) {
            return type == Schema.Type.LONG ? random.nextLong(100) + 1 : random.nextInt(100) + 1;
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
            GenericRecord record,
            Schema schema,
            Map<String, Object> minValues,
            Map<String, Object> maxValues,
            Map<String, Integer> nullCount) {
        
        for (Schema.Field field : schema.getFields()) {
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

