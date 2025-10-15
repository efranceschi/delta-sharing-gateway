package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * EndStreamAction - Common action in the response
 * Used to signal the end of the response stream and provide additional metadata
 * 
 * Based on Delta Sharing Protocol specification:
 * https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#endstreamaction
 * 
 * This action can contain:
 * - refreshToken: Used in snapshot queries to refresh pre-signed URLs
 * - nextPageToken: Used in paginated queries to fetch the next page
 * - minUrlExpirationTimestamp: Minimum expiration timestamp across all URLs
 * - errorMessage: Server error message (if present, client must fail the query)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EndStreamAction {
    
    /**
     * Used in snapshot queries to refresh the pre-signed URLs correctly
     */
    private String refreshToken;
    
    /**
     * Used in paginated queries to fetch the next page correctly
     */
    private String nextPageToken;
    
    /**
     * The minimum unix timestamp (in milliseconds) corresponding to the expiration of the URL,
     * across all URLs in the response
     */
    private Long minUrlExpirationTimestamp;
    
    /**
     * Used by the server to return an error message when an error occurs while handling the request.
     * When this is set, the client must fail the query.
     */
    private String errorMessage;
}

