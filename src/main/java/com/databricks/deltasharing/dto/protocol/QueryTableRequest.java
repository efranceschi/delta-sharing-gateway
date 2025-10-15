package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Request object for querying table data
 * Based on Delta Sharing Protocol specification
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryTableRequest {
    
    /**
     * Predicates for filtering (optional)
     */
    private List<String> predicateHints;
    
    /**
     * Limit on number of files to return (optional)
     */
    private Integer limitHint;
    
    /**
     * Version of the table to query (optional, for time travel)
     */
    private Long version;
    
    /**
     * Timestamp for time travel (optional)
     */
    private String timestamp;
    
    /**
     * Starting version for CDF (Change Data Feed)
     */
    private Long startingVersion;
    
    /**
     * Ending version for CDF (optional)
     */
    private Long endingVersion;
    
    /**
     * Starting timestamp for CDF (optional)
     */
    private String startingTimestamp;
    
    /**
     * Ending timestamp for CDF (optional)
     */
    private String endingTimestamp;
    
    /**
     * Response format requested by client
     * Can be "parquet" or "delta"
     * Reference: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#delta-sharing-capabilities-header
     */
    private String responseFormat;
}
