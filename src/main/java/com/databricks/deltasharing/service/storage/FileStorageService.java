package com.databricks.deltasharing.service.storage;

import com.databricks.deltasharing.dto.protocol.FileResponse;
import com.databricks.deltasharing.model.DeltaTable;

import java.util.List;

/**
 * Interface for file storage services that provide access to Delta table files.
 * Different implementations can provide different storage backends (MinIO, S3, HTTP, etc.)
 */
public interface FileStorageService {
    
    /**
     * Get the list of files for a given Delta table
     * 
     * @param table The Delta table to get files for
     * @param version Optional version of the table (null for latest)
     * @param predicateHints Optional predicate hints for filtering
     * @param limitHint Optional limit hint for the number of files
     * @return List of FileResponse objects containing file metadata and access URLs
     */
    List<FileResponse> getTableFiles(DeltaTable table, Long version, 
                                      List<String> predicateHints, Integer limitHint);
    
    /**
     * Generate or retrieve the schema for a table
     * 
     * @param tableName The name of the table
     * @param format The format of the table (delta or parquet)
     * @return JSON string representation of the schema
     */
    String getTableSchema(String tableName, String format);
    
    /**
     * Get partition columns for a table
     * 
     * @param tableName The name of the table
     * @return Array of partition column names
     */
    String[] getPartitionColumns(String tableName);
    
    /**
     * Get the storage type identifier
     * 
     * @return String identifier for the storage type (e.g., "fake", "minio", "http")
     */
    String getStorageType();
    
    /**
     * Check if this storage service is available and properly configured
     * 
     * @return true if the service is ready to use
     */
    boolean isAvailable();
}
