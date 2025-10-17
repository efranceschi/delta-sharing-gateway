package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response object for a file in Parquet format
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#file-in-parquet-format
 * 
 * According to the protocol, when responseformat=parquet, each file line should be:
 * {
 *   "file": {
 *     "url": "https://...",
 *     "id": "...",
 *     "partitionValues": {...},
 *     "size": 573,
 *     "stats": {...},
 *     "expirationTimestamp": 1652140800000
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
public class ParquetFileResponse {
    
    /**
     * The file object containing all file metadata
     * 
     * This object is the FileResponse with all standard Parquet format fields:
     * - url: Pre-signed URL to download the file
     * - id: Unique identifier for the file
     * - partitionValues: Partition column values
     * - size: File size in bytes
     * - stats: Statistics about the file
     * - expirationTimestamp: When the pre-signed URL expires
     */
    private FileResponse file;
}

