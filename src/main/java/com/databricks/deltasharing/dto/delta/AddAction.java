package com.databricks.deltasharing.dto.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Represents an "add" action from Delta Lake transaction log
 * This indicates a file that is part of the table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AddAction {
    
    /**
     * Relative path to the file from table root
     * Example: "year=2024/month=01/part-00000-abc123.parquet"
     */
    private String path;
    
    /**
     * Partition values for this file
     * Example: {"year": "2024", "month": "01"}
     */
    private Map<String, String> partitionValues;
    
    /**
     * File size in bytes
     */
    private Long size;
    
    /**
     * Modification timestamp (milliseconds since epoch)
     */
    private Long modificationTime;
    
    /**
     * Whether data change occurred (for CDC)
     */
    private Boolean dataChange;
    
    /**
     * Statistics about the data in this file (JSON string)
     * Contains: numRecords, minValues, maxValues, nullCount
     * This field comes as a JSON string in the Delta log
     */
    private String stats;
    
    /**
     * Parsed statistics object (populated from stats JSON string)
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private FileStatistics parsedStats;
    
    /**
     * Additional tags
     */
    private Map<String, String> tags;
}

