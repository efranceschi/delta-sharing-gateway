package com.databricks.deltasharing.dto.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the "metaData" action from Delta Lake transaction log
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetadataAction {
    
    /**
     * Unique identifier for this table
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
     * Format provider (e.g., "parquet")
     */
    private FormatInfo format;
    
    /**
     * Schema string (JSON representation)
     */
    private String schemaString;
    
    /**
     * Partition columns
     */
    @Builder.Default
    private List<String> partitionColumns = new ArrayList<>();
    
    /**
     * Configuration properties
     */
    @Builder.Default
    private Map<String, String> configuration = new HashMap<>();
    
    /**
     * Creation time (milliseconds since epoch)
     */
    private Long createdTime;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class FormatInfo {
        private String provider;
        
        @Builder.Default
        private Map<String, String> options = new HashMap<>();
    }
}

