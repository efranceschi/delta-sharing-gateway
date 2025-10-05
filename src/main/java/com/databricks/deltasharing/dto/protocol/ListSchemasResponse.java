package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response object for listing schemas
 * Based on Delta Sharing Protocol specification
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ListSchemasResponse {
    
    /**
     * List of schemas
     */
    private List<SchemaResponse> items;
    
    /**
     * Token for pagination (optional)
     */
    private String nextPageToken;
}
