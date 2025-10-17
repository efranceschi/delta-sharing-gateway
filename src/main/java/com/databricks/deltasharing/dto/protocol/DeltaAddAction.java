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
     * Size of the file in bytes (optional)
     * This is the uncompressed size of the Parquet file
     */
    private Long size;
    
    /**
     * The time when this file was created/modified, as milliseconds since the epoch
     * This is the modification timestamp of the file in the underlying storage system
     * Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
     */
    private Long modificationTime;
    
    /**
     * Whether this file contains data changes (true) or only metadata changes (false)
     * When true, this indicates that the file contains actual data rows
     * When false, this indicates that the file only contains metadata or is part of a transaction
     * Default: true (most files contain data changes)
     * Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
     */
    private Boolean dataChange;
    
    /**
     * File statistics as a JSON STRING (not an object)
     * Example: "{\"numRecords\":1,\"minValues\":{...},\"maxValues\":{...},\"nullCount\":{...}}"
     */
    private String stats;
}

