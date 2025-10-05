package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Metadata information for Delta table
 * Based on Delta Sharing Protocol specification
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetadataResponse {
    
    /**
     * Unique identifier for the table
     */
    private String id;
    
    /**
     * Table name
     */
    private String name;
    
    /**
     * Table description
     */
    private String description;
    
    /**
     * Format of the table
     */
    private FormatResponse format;
    
    /**
     * Schema string in JSON format
     */
    private String schemaString;
    
    /**
     * Partition columns
     */
    private List<String> partitionColumns;
    
    /**
     * Configuration properties
     */
    private Map<String, String> configuration;
    
    /**
     * Table version
     */
    private Long version;
    
    /**
     * Table size in bytes
     */
    private Long size;
    
    /**
     * Number of files
     */
    private Long numFiles;
}
