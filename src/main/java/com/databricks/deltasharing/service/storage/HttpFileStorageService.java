package com.databricks.deltasharing.service.storage;

import com.databricks.deltasharing.dto.delta.AddAction;
import com.databricks.deltasharing.dto.delta.DeltaSnapshot;
import com.databricks.deltasharing.dto.protocol.FileResponse;
import com.databricks.deltasharing.model.DeltaTable;
import com.databricks.deltasharing.service.delta.DataSkippingService;
import com.databricks.deltasharing.service.delta.DeltaLogReader;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HTTP-based file storage service implementation.
 * Serves files through a generic HTTP server without temporary token signing.
 * URLs are constructed based on a configurable base URL and file paths.
 * 
 * Supports Delta Lake transaction log reading and data skipping.
 */
@Service
@Slf4j
@Setter
@ConditionalOnProperty(name = "delta.sharing.storage.type", havingValue = "http")
@ConfigurationProperties(prefix = "delta.sharing.storage.http")
public class HttpFileStorageService implements FileStorageService {
    
    private String baseUrl;
    private String basePath;
    private boolean enabled = true;
    private boolean useDeltaLog = true; // Use Delta transaction log by default
    
    @Autowired(required = false)
    private DeltaLogReader deltaLogReader;
    
    @Autowired(required = false)
    private DataSkippingService dataSkippingService;
    
    @Override
    public List<FileResponse> getTableFiles(DeltaTable table, Long version, 
                                             List<String> predicateHints, Integer limitHint) {
        log.debug("Getting HTTP files for table: {} at location: {}", 
                  table.getName(), table.getLocation());
        
        if (!isAvailable()) {
            log.warn("HTTP storage service is not properly configured");
            return new ArrayList<>();
        }
        
        // Try to use Delta transaction log if enabled
        if (useDeltaLog && deltaLogReader != null && dataSkippingService != null) {
            try {
                return getTableFilesFromDeltaLog(table, version, predicateHints, limitHint);
            } catch (Exception e) {
                log.warn("Failed to read Delta log, falling back to direct file listing: {}", e.getMessage());
                log.debug("Delta log error details", e);
            }
        }
        
        // Fallback: list files directly from filesystem (legacy mode, no data skipping)
        log.info("Using legacy mode (no Delta log) for table: {}", table.getName());
        return getTableFilesLegacy(table, version, limitHint);
    }
    
    /**
     * Get table files from Delta transaction log (preferred method)
     * Supports data skipping with predicate pushdown
     */
    private List<FileResponse> getTableFilesFromDeltaLog(DeltaTable table, Long version,
                                                          List<String> predicateHints, Integer limitHint) throws Exception {
        String tableLocation = resolveTableLocation(table);
        Path tablePath = Paths.get(tableLocation);
        
        log.debug("Reading Delta log from filesystem: {}", tableLocation);
        
        // Read Delta log from filesystem
        DeltaSnapshot snapshot = deltaLogReader.readDeltaLog(tableLocation, version);
        
        log.info("Delta log read: {} files in snapshot (version={})", 
                 snapshot.getFileCount(), version);
        
        // Apply data skipping with predicates
        List<AddAction> allFiles = snapshot.getAddActions();
        List<AddAction> filteredFiles = dataSkippingService.applyDataSkipping(allFiles, predicateHints);
        
        // Apply limit hint
        int limit = limitHint != null && limitHint > 0 ? limitHint : Integer.MAX_VALUE;
        List<AddAction> limitedFiles = filteredFiles.stream()
                .limit(limit)
                .collect(Collectors.toList());
        
        log.info("Returning {} files after data skipping and limit (from {} total)", 
                 limitedFiles.size(), allFiles.size());
        
        // Convert AddAction to FileResponse with HTTP URLs
        return limitedFiles.stream()
                .map(addAction -> createFileResponseFromAddAction(addAction, tablePath, version))
                .filter(response -> response != null)
                .collect(Collectors.toList());
    }
    
    /**
     * Legacy method: list files directly from filesystem without Delta log
     * Does NOT support data skipping (predicates are ignored)
     */
    private List<FileResponse> getTableFilesLegacy(DeltaTable table, Long version, Integer limitHint) {
        log.warn("Legacy mode does not support data skipping - predicates will be ignored");
        
        List<FileResponse> files = new ArrayList<>();
        String tableLocation = resolveTableLocation(table);
        
        try {
            Path tablePath = Paths.get(tableLocation);
            
            if (!Files.exists(tablePath)) {
                log.warn("Table location does not exist: {}", tableLocation);
                return files;
            }
            
            // Legacy: just list all Parquet files (no Delta log, no data skipping)
            log.error("Legacy mode is deprecated. Please ensure Delta transaction log is available.");
            
        } catch (Exception e) {
            log.error("Error listing files for table: {}", table.getName(), e);
        }
        
        return files;
    }
    
    /**
     * Create FileResponse from AddAction (Delta log)
     */
    private FileResponse createFileResponseFromAddAction(AddAction addAction, Path tablePath, Long version) {
        try {
            Path filePath = tablePath.resolve(addAction.getPath());
            Path relativePath = tablePath.relativize(filePath);
            
            // Construct HTTP URL
            String urlPath = relativePath.toString().replace(File.separator, "/");
            String fileUrl = baseUrl.endsWith("/") ? 
                             baseUrl + urlPath : 
                             baseUrl + "/" + urlPath;
            
            // Convert FileStatistics to Map for response
            Map<String, Object> stats = null;
            if (addAction.getParsedStats() != null) {
                stats = new HashMap<>();
                stats.put("numRecords", addAction.getParsedStats().getNumRecords());
                if (addAction.getParsedStats().getMinValues() != null) {
                    stats.put("minValues", addAction.getParsedStats().getMinValues());
                }
                if (addAction.getParsedStats().getMaxValues() != null) {
                    stats.put("maxValues", addAction.getParsedStats().getMaxValues());
                }
                if (addAction.getParsedStats().getNullCount() != null) {
                    stats.put("nullCount", addAction.getParsedStats().getNullCount());
                }
            }
            
            return FileResponse.builder()
                    .url(fileUrl)
                    .id(addAction.getPath().replace("/", "-").replace(".parquet", ""))
                    .partitionValues(addAction.getPartitionValues())
                    .size(addAction.getSize())
                    .stats(stats)
                    .version(version != null ? version : 0L)
                    .timestamp(addAction.getModificationTime() != null ? 
                              addAction.getModificationTime() : System.currentTimeMillis())
                    .build();
                    
        } catch (Exception e) {
            log.error("Failed to create FileResponse for: {}", addAction.getPath(), e);
            return null;
        }
    }
    
    private String resolveTableLocation(DeltaTable table) {
        String location = table.getLocation();
        
        if (location == null || location.isEmpty()) {
            location = table.getName();
        }
        
        // Handle s3:// prefix - extract path after bucket
        if (location.startsWith("s3://")) {
            String withoutProtocol = location.substring(5); // Remove "s3://"
            int firstSlash = withoutProtocol.indexOf('/');
            
            if (firstSlash > 0) {
                location = withoutProtocol.substring(firstSlash + 1); // Path after bucket
                log.debug("Resolved S3 location to local path: {}", location);
            } else {
                // Only bucket name, use table name
                location = table.getName();
            }
        }
        
        // If location is absolute, use it directly
        if (location.startsWith("/") || location.matches("^[a-zA-Z]:.*")) {
            return location;
        }
        
        // Otherwise, combine with base path
        if (basePath != null && !basePath.isEmpty()) {
            return Paths.get(basePath, location).toString();
        }
        
        return location;
    }
    
    @Override
    public String getStorageType() {
        return "http";
    }
    
    @Override
    public boolean isAvailable() {
        return enabled && baseUrl != null && !baseUrl.isEmpty();
    }
}
