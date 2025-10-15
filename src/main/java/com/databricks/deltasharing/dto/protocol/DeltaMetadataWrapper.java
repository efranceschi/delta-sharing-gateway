package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wrapper for Delta Metadata when responseformat=delta
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#delta-sharing-capabilities-header
 * 
 * Format: {"metaData": {"deltaMetadata": {...}}}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeltaMetadataWrapper {
    
    /**
     * Wrapped delta metadata object
     */
    private MetadataResponse deltaMetadata;
}

