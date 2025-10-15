package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents the "deltaSingleAction" in Delta Sharing Protocol for delta format
 * Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md
 * 
 * Contains the "add" action with file details
 * Used when responseformat=delta
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeltaSingleAction {
    
    /**
     * The add action containing file details
     */
    private DeltaAddAction add;
}

