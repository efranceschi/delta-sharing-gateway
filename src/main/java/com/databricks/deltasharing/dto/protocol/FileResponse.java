package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * File information for Delta table
 * Based on Delta Sharing Protocol specification
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FileResponse {
    
    /**
     * URL to access the file
     */
    private String url;
    
    /**
     * Unique identifier for the file
     */
    private String id;
    
    /**
     * Partition values for the file
     */
    private Map<String, String> partitionValues;
    
    /**
     * File size in bytes
     */
    private Long size;
    
    /**
     * File statistics (optional)
     */
    private Map<String, Object> stats;
    
    /**
     * Expiration timestamp for the URL
     */
    private Long expirationTimestamp;
    
    /**
     * File version
     */
    private Long version;
    
    /**
     * Timestamp when the file was added
     */
    private Long timestamp;
    
    /**
     * Type of change (for CDF - Change Data Feed)
     * Values: "insert", "update", "delete"
     */
    private String changeType;
}
