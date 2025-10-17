package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wrapper for Protocol response in NDJSON format
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
 * 
 * This represents the first line in NDJSON responses for metadata, query, and changes endpoints.
 * 
 * For Parquet format:
 * {
 *   "protocol": {
 *     "minReaderVersion": 1
 *   }
 * }
 * 
 * For Delta format:
 * {
 *   "protocol": {
 *     "deltaProtocol": {
 *       "minReaderVersion": 1,
 *       "minWriterVersion": 2
 *     }
 *   }
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProtocolWrapper {
    
    /**
     * Protocol object for Parquet format (direct ProtocolResponse)
     * OR
     * Protocol object for Delta format (wrapped in DeltaProtocolWrapper)
     * 
     * Note: This field can hold either:
     * - ProtocolResponse (for Parquet format)
     * - DeltaProtocolWrapper (for Delta format)
     * 
     * Jackson will serialize the appropriate object based on what is set.
     */
    private Object protocol;
}

