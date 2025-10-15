package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wrapper for File response in Delta format
 * Used when responseformat=delta for /query endpoint
 * Format: {"file": {"deltaSingleAction": {...}}}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeltaSingleActionWrapper {
    private FileResponse deltaSingleAction;
}

