package com.databricks.deltasharing.dto.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Statistics about data in a Parquet file
 * Used for data skipping (partition pruning and min/max filtering)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FileStatistics {
    
    /**
     * Total number of records in the file
     */
    private Long numRecords;
    
    /**
     * Minimum values for each column
     * Example: {"id": 0, "price": 10.5, "date": "2024-01-01"}
     */
    private Map<String, Object> minValues;
    
    /**
     * Maximum values for each column
     * Example: {"id": 999, "price": 199.99, "date": "2024-12-31"}
     */
    private Map<String, Object> maxValues;
    
    /**
     * Null count for each column
     * Example: {"id": 0, "name": 5, "description": 100}
     */
    private Map<String, Long> nullCount;
}

