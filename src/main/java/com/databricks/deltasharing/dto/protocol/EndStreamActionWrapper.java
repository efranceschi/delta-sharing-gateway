package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wrapper for EndStreamAction response
 * Based on Delta Sharing Protocol specification
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#endstreamaction
 * 
 * Format: {"endStreamAction": {"refreshToken": "...", "minUrlExpirationTimestamp": ...}}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EndStreamActionWrapper {
    
    /**
     * The EndStreamAction object wrapped
     */
    private EndStreamAction endStreamAction;
}

