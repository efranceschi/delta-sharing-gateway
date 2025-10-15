package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wrapper for Delta Single Action when responseformat=delta
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#delta-sharing-capabilities-header
 * 
 * Format: {"file": {"deltaSingleAction": {...}}}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeltaSingleActionWrapper {
    
    /**
     * Wrapped delta single action (file) object
     */
    private FileResponse deltaSingleAction;
}

