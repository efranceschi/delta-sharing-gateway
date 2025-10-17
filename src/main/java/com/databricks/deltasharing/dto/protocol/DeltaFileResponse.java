package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response object for a file in Delta format
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#file-in-delta-format
 * 
 * According to the protocol, when responseformat=delta, each file line should be:
 * {
 *   "file": {
 *     "id": "...",
 *     "size": 573,
 *     "expirationTimestamp": 1652140800000,
 *     "deltaSingleAction": {
 *       "add": {
 *         "path": "https://...",
 *         "partitionValues": {...},
 *         "size": 573,
 *         "stats": "{\"numRecords\":1,...}",
 *         "deletionVector": {...}  // optional
 *       }
 *     }
 *   }
 * }
 * 
 * This class represents the outer wrapper with the "file" key.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeltaFileResponse {
    
    /**
     * The file object containing all file metadata and delta action
     * 
     * This object includes:
     * - id: Unique identifier for the file
     * - size: File size in bytes
     * - expirationTimestamp: When the pre-signed URL expires
     * - deltaSingleAction: The Delta Lake action (add/remove)
     */
    private DeltaSingleActionWrapper file;
}

