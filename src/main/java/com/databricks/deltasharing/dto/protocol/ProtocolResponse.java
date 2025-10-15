package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Protocol information for Delta Sharing
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#read-data-from-a-table
 * 
 * Format: {"protocol": {"minReaderVersion": 1}}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProtocolResponse {
    
    /**
     * Minimum reader version required by Delta Sharing Protocol
     * Always set to 1 for current specification
     */
    private Integer minReaderVersion;
}
