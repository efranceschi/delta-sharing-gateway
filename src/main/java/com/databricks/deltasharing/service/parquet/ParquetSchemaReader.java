package com.databricks.deltasharing.service.parquet;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Service for reading schemas from Parquet files
 * Converts Parquet schema to Delta Sharing JSON format
 */
@Slf4j
@Service
public class ParquetSchemaReader {
    
    private static final Configuration HADOOP_CONF = new Configuration();
    
    /**
     * Read schema from a Parquet file on local filesystem
     * 
     * @param filePath Path to the Parquet file
     * @return JSON schema string in Delta Sharing format
     * @throws IOException if file cannot be read
     */
    public String readSchemaFromFile(String filePath) throws IOException {
        return readSchemaFromFile(new File(filePath));
    }
    
    /**
     * Read schema from a Parquet file
     * 
     * @param file Parquet file
     * @return JSON schema string in Delta Sharing format
     * @throws IOException if file cannot be read
     */
    public String readSchemaFromFile(File file) throws IOException {
        if (!file.exists()) {
            throw new IOException("Parquet file not found: " + file.getAbsolutePath());
        }
        
        if (!file.getName().endsWith(".parquet")) {
            throw new IOException("File is not a Parquet file: " + file.getName());
        }
        
        log.debug("Reading Parquet schema from: {}", file.getAbsolutePath());
        
        try {
            Path hadoopPath = new Path(file.getAbsolutePath());
            HadoopInputFile inputFile = HadoopInputFile.fromPath(hadoopPath, HADOOP_CONF);
            
            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
                log.debug("Parquet schema read successfully: {}", parquetSchema.getName());
                
                return convertParquetSchemaToJson(parquetSchema);
            }
        } catch (Exception e) {
            log.error("Failed to read Parquet schema from file: {}", file.getAbsolutePath(), e);
            throw new IOException("Failed to read Parquet schema: " + e.getMessage(), e);
        }
    }
    
    /**
     * Read schema from a Parquet InputStream
     * This is useful for reading from MinIO/S3
     * 
     * @param inputStream InputStream containing Parquet data
     * @param fileName Name of the file (for logging)
     * @return JSON schema string in Delta Sharing format
     * @throws IOException if stream cannot be read
     */
    public String readSchemaFromStream(InputStream inputStream, String fileName) throws IOException {
        // Write stream to temporary file
        File tempFile = Files.createTempFile("parquet-schema-", ".parquet").toFile();
        tempFile.deleteOnExit();
        
        try {
            Files.copy(inputStream, tempFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return readSchemaFromFile(tempFile);
        } finally {
            if (tempFile.exists()) {
                tempFile.delete();
            }
        }
    }
    
    /**
     * Convert Parquet MessageType to Delta Sharing JSON schema format
     * 
     * @param parquetSchema Parquet schema
     * @return JSON schema string
     */
    private String convertParquetSchemaToJson(MessageType parquetSchema) {
        List<String> fields = new ArrayList<>();
        
        for (Type field : parquetSchema.getFields()) {
            fields.add(convertFieldToJson(field));
        }
        
        return String.format("{\"type\":\"struct\",\"fields\":[%s]}", String.join(",", fields));
    }
    
    /**
     * Convert a Parquet field to JSON format
     * 
     * @param field Parquet field
     * @return JSON field string
     */
    private String convertFieldToJson(Type field) {
        String name = field.getName();
        String type = convertParquetTypeToSparkType(field);
        boolean nullable = !field.isRepetition(Type.Repetition.REQUIRED);
        
        // Build metadata (empty for now, but can be extended)
        String metadata = "{}";
        
        return String.format(
            "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":%s,\"metadata\":%s}",
            name, type, nullable, metadata
        );
    }
    
    /**
     * Convert Parquet type to Spark SQL type
     * 
     * @param field Parquet field
     * @return Spark SQL type string
     */
    private String convertParquetTypeToSparkType(Type field) {
        if (field.isPrimitive()) {
            return convertPrimitiveType(field);
        } else {
            // Complex type (struct, array, map)
            return convertComplexType(field);
        }
    }
    
    /**
     * Convert Parquet primitive type to Spark SQL type
     */
    private String convertPrimitiveType(Type field) {
        org.apache.parquet.schema.PrimitiveType primitiveType = field.asPrimitiveType();
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
        
        return switch (typeName) {
            case BOOLEAN -> "boolean";
            case INT32 -> {
                // Check for logical type
                if (primitiveType.getLogicalTypeAnnotation() != null) {
                    String logicalType = primitiveType.getLogicalTypeAnnotation().toString();
                    if (logicalType.contains("DATE")) {
                        yield "date";
                    } else if (logicalType.contains("INT_8") || logicalType.contains("INT(8")) {
                        yield "byte";
                    } else if (logicalType.contains("INT_16") || logicalType.contains("INT(16")) {
                        yield "short";
                    }
                }
                yield "integer";
            }
            case INT64 -> {
                // Check for logical type (timestamp)
                if (primitiveType.getLogicalTypeAnnotation() != null) {
                    String logicalType = primitiveType.getLogicalTypeAnnotation().toString();
                    if (logicalType.contains("TIMESTAMP")) {
                        yield "timestamp";
                    }
                }
                yield "long";
            }
            case INT96 -> "timestamp"; // Legacy timestamp format
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case BINARY -> {
                // Check for logical type (string, decimal)
                if (primitiveType.getLogicalTypeAnnotation() != null) {
                    String logicalType = primitiveType.getLogicalTypeAnnotation().toString();
                    if (logicalType.contains("STRING") || logicalType.contains("UTF8")) {
                        yield "string";
                    } else if (logicalType.contains("DECIMAL")) {
                        // Extract precision and scale if available
                        yield "decimal(10,0)"; // Default, should parse from annotation
                    }
                }
                yield "binary";
            }
            case FIXED_LEN_BYTE_ARRAY -> {
                // Usually decimal
                if (primitiveType.getLogicalTypeAnnotation() != null) {
                    String logicalType = primitiveType.getLogicalTypeAnnotation().toString();
                    if (logicalType.contains("DECIMAL")) {
                        yield "decimal(10,0)"; // Default, should parse from annotation
                    }
                }
                yield "binary";
            }
            default -> "string"; // Fallback
        };
    }
    
    /**
     * Convert Parquet complex type to Spark SQL type
     */
    private String convertComplexType(Type field) {
        GroupType groupType = field.asGroupType();
        
        // Check for LIST annotation
        if (groupType.getLogicalTypeAnnotation() != null) {
            String logicalType = groupType.getLogicalTypeAnnotation().toString();
            if (logicalType.contains("LIST")) {
                // Array type
                if (groupType.getFieldCount() > 0) {
                    Type elementField = groupType.getFields().get(0);
                    if (elementField.isRepetition(Type.Repetition.REPEATED)) {
                        String elementType = convertParquetTypeToSparkType(elementField);
                        return String.format("array<%s>", elementType);
                    }
                }
                return "array<string>"; // Default
            } else if (logicalType.contains("MAP")) {
                // Map type
                return "map<string,string>"; // Simplified, should parse key/value types
            }
        }
        
        // Struct type
        List<String> structFields = new ArrayList<>();
        for (Type subField : groupType.getFields()) {
            structFields.add(convertFieldToJson(subField));
        }
        
        return String.format("struct<%s>", String.join(",", structFields));
    }
    
    /**
     * Find first Parquet file in a directory
     * 
     * @param directory Directory to search
     * @return First Parquet file found, or null if none
     */
    public File findFirstParquetFile(File directory) {
        if (!directory.exists() || !directory.isDirectory()) {
            log.warn("Directory does not exist or is not a directory: {}", directory.getAbsolutePath());
            return null;
        }
        
        File[] files = directory.listFiles((dir, name) -> name.endsWith(".parquet"));
        
        if (files != null && files.length > 0) {
            log.debug("Found {} Parquet file(s) in directory: {}", files.length, directory.getAbsolutePath());
            return files[0]; // Return first file
        }
        
        log.warn("No Parquet files found in directory: {}", directory.getAbsolutePath());
        return null;
    }
}

