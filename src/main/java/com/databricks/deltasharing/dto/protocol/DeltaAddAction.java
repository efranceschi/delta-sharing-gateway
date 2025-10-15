package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Represents the "add" action in Delta Sharing Protocol for delta format
 * Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
 * 
 * Used when responseformat=delta
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeltaAddAction {
    
    /**
     * The path or URL to the file
     */
    private String path;
    
    /**
     * Partition values for the file
     */
    private Map<String, String> partitionValues;
    
    /**
     * File statistics as a JSON STRING (not an object)
     * Example: "{\"numRecords\":1,\"minValues\":{...},\"maxValues\":{...},\"nullCount\":{...}}"
     */
    private String stats;
}

