package com.databricks.deltasharing.dto.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents the "protocol" action from Delta Lake transaction log
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProtocolAction {
    
    /**
     * Minimum reader version required
     */
    private Integer minReaderVersion;
    
    /**
     * Minimum writer version required
     */
    private Integer minWriterVersion;
}

