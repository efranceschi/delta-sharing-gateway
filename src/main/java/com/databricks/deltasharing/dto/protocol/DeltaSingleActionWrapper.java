package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wrapper for Delta Single Action when responseformat=delta
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
 * 
 * Format according to protocol:
 * {
 *   "file": {
 *     "id": "...",
 *     "size": 573,
 *     "expirationTimestamp": 1652140800000,
 *     "deltaSingleAction": {
 *       "add": {
 *         "path": "https://...",
 *         "partitionValues": {...},
 *         "stats": "{\"numRecords\":1,...}"
 *       }
 *     }
 *   }
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeltaSingleActionWrapper {
    
    /**
     * Unique identifier for the file
     */
    private String id;
    
    /**
     * File size in bytes
     */
    private Long size;
    
    /**
     * Expiration timestamp for the URL
     */
    private Long expirationTimestamp;
    
    /**
     * The delta single action containing the "add" action
     */
    private DeltaSingleAction deltaSingleAction;
}

