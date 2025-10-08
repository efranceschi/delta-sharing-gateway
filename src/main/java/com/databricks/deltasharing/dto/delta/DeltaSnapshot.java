package com.databricks.deltasharing.dto.delta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a snapshot of a Delta table at a specific version
 * Contains all active files (add actions minus remove actions)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeltaSnapshot {
    
    /**
     * Version number of this snapshot
     */
    private Long version;
    
    /**
     * Protocol information (minReaderVersion, minWriterVersion)
     */
    private ProtocolAction protocol;
    
    /**
     * Metadata information (id, name, schema, partitionColumns, etc)
     */
    private MetadataAction metadata;
    
    /**
     * All active files in this snapshot (after applying removes)
     */
    @Builder.Default
    private List<AddAction> addActions = new ArrayList<>();
    
    /**
     * Total number of files in snapshot
     */
    public int getFileCount() {
        return addActions != null ? addActions.size() : 0;
    }
}

