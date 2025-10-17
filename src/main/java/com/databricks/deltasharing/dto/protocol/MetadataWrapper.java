package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wrapper for Metadata response in NDJSON format
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
 * 
 * This represents the second line in NDJSON responses for metadata, query, and changes endpoints.
 * 
 * For Parquet format:
 * {
 *   "metaData": {
 *     "id": "...",
 *     "name": "...",
 *     "format": {...},
 *     "schemaString": "...",
 *     "partitionColumns": [...],
 *     "size": 123,
 *     "numFiles": 1,
 *     ...
 *   }
 * }
 * 
 * For Delta format:
 * {
 *   "metaData": {
 *     "size": 123,
 *     "numFiles": 1,
 *     "deltaMetadata": {
 *       "id": "...",
 *       "name": "...",
 *       "format": {...},
 *       "schemaString": "...",
 *       "partitionColumns": [...],
 *       ...
 *     }
 *   }
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MetadataWrapper {
    
    /**
     * Metadata object for Parquet format (direct MetadataResponse)
     * OR
     * Metadata object for Delta format (wrapped in DeltaMetadataWrapper)
     * 
     * Note: This field can hold either:
     * - MetadataResponse (for Parquet format)
     * - DeltaMetadataWrapper (for Delta format)
     * 
     * Jackson will serialize the appropriate object based on what is set.
     */
    private Object metaData;
}

