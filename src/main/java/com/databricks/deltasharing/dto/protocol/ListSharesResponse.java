package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response object for listing shares
 * Based on Delta Sharing Protocol specification
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ListSharesResponse {
    
    /**
     * List of shares
     */
    private List<ShareResponse> items;
    
    /**
     * Token for pagination (optional)
     */
    private String nextPageToken;
}
