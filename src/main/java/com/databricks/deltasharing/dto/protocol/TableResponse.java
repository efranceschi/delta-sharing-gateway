package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response object for Table information
 * Based on Delta Sharing Protocol specification
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableResponse {
    
    /**
     * The name of the table
     */
    private String name;
    
    /**
     * The name of the schema
     */
    private String schema;
    
    /**
     * The name of the share
     */
    private String share;
    
    /**
     * Whether the table is shared as a view
     */
    private Boolean shareAsView;
    
    /**
     * Optional table ID
     */
    private String id;
}
