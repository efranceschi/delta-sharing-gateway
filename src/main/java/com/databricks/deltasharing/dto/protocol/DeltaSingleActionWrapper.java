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
     * A unique string for the deletion vector file in a table.
     * The same deletion vector file is guaranteed to have the same id across multiple requests.
     * A client may cache the file content and use this id as a key to decide whether to use the cached file content.
     * Optional: Only present when deletion vectors are enabled and the file has deletion vectors.
     */
    private String deletionVectorFileId;

    /**
     * The table version of the file.
     * Returned when querying a table data with a version or timestamp parameter.
     * Optional: Only present when querying with version/timestamp.
     */
    private Long version;

    /**
     * The unix timestamp corresponding to the table version of the file, in milliseconds.
     * Returned when querying a table data with a version or timestamp parameter.
     * Optional: Only present when querying with version/timestamp.
     */
    private Long timestamp;

    /**
     * The unix timestamp corresponding to the expiration of the url, in milliseconds.
     * Returned when the server supports the feature.
     * This indicates when the pre-signed URL will expire and needs to be refreshed.
     */
    private Long expirationTimestamp;
    
    /**
     * Need to be parsed by a delta library as a delta single action, the path field is replaced by pr-signed url.
     */
    private DeltaSingleAction deltaSingleAction;
}

