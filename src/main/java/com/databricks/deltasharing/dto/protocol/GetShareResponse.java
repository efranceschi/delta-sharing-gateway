package com.databricks.deltasharing.dto.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response wrapper for GET /shares/{share} endpoint
 * Based on Delta Sharing Protocol specification
 * 
 * According to the official protocol, the response must wrap the share
 * object in a "share" field:
 * {
 *   "share": {
 *     "name": "share_name",
 *     "id": "share_id"
 *   }
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetShareResponse {
    
    /**
     * Share information wrapped in "share" field per protocol specification
     */
    private ShareResponse share;
}

