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
 *     "expirationTimestamp": 1652140800000,
 *     "deltaSingleAction": {
 *       "add": {
 *         "path": "https://...",
 *         "partitionValues": {...},
 *         "size": 573,
 *         "modificationTime": 1652140800000,
 *         "dataChange": true,
 *         "stats": "{\"numRecords\":1,...}"
 *       }
 *     },
 *     "url": "https://..."
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
     * A unique string for the file in a table
     */
    private String id;

    /**
     * The unix timestamp corresponding to the expiration of the url, in milliseconds.
     * Returned when the server supports the feature.
     * This indicates when the pre-signed URL will expire and needs to be refreshed.
     */
    private Long expirationTimestamp;
    
    /**
     * Need to be parsed by a delta library as a delta single action, the path field is replaced by pre-signed url.
     */
    private DeltaSingleAction deltaSingleAction;
    
    /**
     * Pre-signed URL for accessing the file
     * This is the same URL that appears in deltaSingleAction.add.path
     * Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#file-in-delta-format
     */
    private String url;
}

