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
     * File statistics as a JSON STRING (not an object)
     * Example: "{\"numRecords\":1,\"minValues\":{...},\"maxValues\":{...},\"nullCount\":{...}}"
     */
    private String stats;
    
    /**
     * Information about deletion vectors for this file (optional)
     * Only present when the file has deletion vectors and client requested readerfeatures=deletionvectors
     * 
     * Format:
     * {
     *   "storageType": "u",
     *   "pathOrInlineDv": "vBn[lx{q8@P<9BNH/isA",
     *   "offset": 1,
     *   "sizeInBytes": 36,
     *   "cardinality": 2
     * }
     */
    private DeletionVectorDescriptor deletionVector;
}

