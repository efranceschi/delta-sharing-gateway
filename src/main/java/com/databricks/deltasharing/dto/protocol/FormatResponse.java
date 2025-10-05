package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Format information for Delta table
 * Based on Delta Sharing Protocol specification
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FormatResponse {
    
    /**
     * The provider of the format (e.g., "parquet")
     */
    private String provider;
    
    /**
     * Additional format options
     */
    private Object options;
}
