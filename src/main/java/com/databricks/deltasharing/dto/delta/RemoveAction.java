package com.databricks.deltasharing.dto.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a "remove" action from Delta Lake transaction log
 * This indicates a file that was removed from the table (tombstone)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RemoveAction {
    
    /**
     * Relative path to the file that was removed
     */
    private String path;
    
    /**
     * Deletion timestamp (milliseconds since epoch)
     */
    private Long deletionTimestamp;
    
    /**
     * Whether data change occurred (for CDC)
     */
    private Boolean dataChange;
}

