package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wrapper for Protocol response in Delta format
 * Used when responseformat=delta for /query endpoint
 * Format: {"protocol": {"deltaProtocol": {...}}}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeltaProtocolWrapper {
    private ProtocolResponse deltaProtocol;
}

