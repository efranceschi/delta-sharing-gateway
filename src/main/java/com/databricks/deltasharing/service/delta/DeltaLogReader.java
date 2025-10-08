package com.databricks.deltasharing.service.delta;

import com.databricks.deltasharing.dto.delta.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for reading Delta Lake transaction logs
 * Reads _delta_log/*.json files and constructs table snapshots
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeltaLogReader {
    
    private final ObjectMapper objectMapper;
    
    /**
     * Read Delta transaction log from filesystem
     * 
     * @param tablePath Path to the table root directory
     * @param version Optional version number (null for latest)
     * @return DeltaSnapshot containing active files
     */
    public DeltaSnapshot readDeltaLog(String tablePath, Long version) throws IOException {
        Path deltaLogPath = Paths.get(tablePath, "_delta_log");
        
        if (!Files.exists(deltaLogPath)) {
            log.warn("Delta log directory not found: {}", deltaLogPath);
            return createEmptySnapshot(version);
        }
        
        // Find the version file to read
        Long targetVersion = version != null ? version : findLatestVersion(deltaLogPath);
        Path logFile = deltaLogPath.resolve(String.format("%020d.json", targetVersion));
        
        if (!Files.exists(logFile)) {
            log.warn("Delta log file not found: {}", logFile);
            return createEmptySnapshot(version);
        }
        
        log.debug("Reading Delta log: {}", logFile);
        
        return parseDeltaLog(logFile, targetVersion);
    }
    
    /**
     * Read Delta transaction log from InputStream (for MinIO/S3)
     * 
     * @param inputStream InputStream containing the log file
     * @param version Version number
     * @return DeltaSnapshot containing active files
     */
    public DeltaSnapshot readDeltaLog(InputStream inputStream, Long version) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            
            return parseDeltaLog(reader, version);
        }
    }
    
    /**
     * Parse Delta log file (NDJSON format)
     */
    private DeltaSnapshot parseDeltaLog(Path logFile, Long version) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)) {
            return parseDeltaLog(reader, version);
        }
    }
    
    /**
     * Parse Delta log from BufferedReader
     */
    private DeltaSnapshot parseDeltaLog(BufferedReader reader, Long version) throws IOException {
        ProtocolAction protocol = null;
        MetadataAction metadata = null;
        Map<String, AddAction> addActions = new HashMap<>();
        Set<String> removedPaths = new HashSet<>();
        
        String line;
        int lineNumber = 0;
        
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            line = line.trim();
            
            if (line.isEmpty()) {
                continue;
            }
            
            try {
                JsonNode node = objectMapper.readTree(line);
                
                // Parse protocol action
                if (node.has("protocol")) {
                    protocol = parseProtocolAction(node.get("protocol"));
                    log.debug("Found protocol: minReader={}, minWriter={}", 
                             protocol.getMinReaderVersion(), 
                             protocol.getMinWriterVersion());
                }
                
                // Parse metadata action
                else if (node.has("metaData")) {
                    metadata = parseMetadataAction(node.get("metaData"));
                    log.debug("Found metadata: id={}, name={}, partitions={}", 
                             metadata.getId(), 
                             metadata.getName(),
                             metadata.getPartitionColumns());
                }
                
                // Parse add action
                else if (node.has("add")) {
                    AddAction add = parseAddAction(node.get("add"));
                    addActions.put(add.getPath(), add);
                    log.trace("Add file: {}", add.getPath());
                }
                
                // Parse remove action (tombstone)
                else if (node.has("remove")) {
                    RemoveAction remove = parseRemoveAction(node.get("remove"));
                    removedPaths.add(remove.getPath());
                    addActions.remove(remove.getPath());
                    log.trace("Remove file: {}", remove.getPath());
                }
                
            } catch (Exception e) {
                log.error("Error parsing Delta log line {}: {}", lineNumber, line, e);
                // Continue parsing other lines
            }
        }
        
        // Build snapshot with active files only (adds minus removes)
        List<AddAction> activeFiles = addActions.values().stream()
                .filter(add -> !removedPaths.contains(add.getPath()))
                .collect(Collectors.toList());
        
        log.info("Delta log parsed: version={}, files={} (added={}, removed={})", 
                 version, activeFiles.size(), addActions.size(), removedPaths.size());
        
        return DeltaSnapshot.builder()
                .version(version)
                .protocol(protocol)
                .metadata(metadata)
                .addActions(activeFiles)
                .build();
    }
    
    /**
     * Parse protocol action
     */
    private ProtocolAction parseProtocolAction(JsonNode node) {
        return ProtocolAction.builder()
                .minReaderVersion(node.has("minReaderVersion") ? node.get("minReaderVersion").asInt() : 1)
                .minWriterVersion(node.has("minWriterVersion") ? node.get("minWriterVersion").asInt() : 1)
                .build();
    }
    
    /**
     * Parse metadata action
     */
    private MetadataAction parseMetadataAction(JsonNode node) throws IOException {
        MetadataAction.MetadataActionBuilder builder = MetadataAction.builder()
                .id(node.has("id") ? node.get("id").asText() : null)
                .name(node.has("name") ? node.get("name").asText() : null)
                .description(node.has("description") ? node.get("description").asText() : null)
                .schemaString(node.has("schemaString") ? node.get("schemaString").asText() : null)
                .createdTime(node.has("createdTime") ? node.get("createdTime").asLong() : null);
        
        // Parse format
        if (node.has("format")) {
            JsonNode formatNode = node.get("format");
            MetadataAction.FormatInfo format = MetadataAction.FormatInfo.builder()
                    .provider(formatNode.has("provider") ? formatNode.get("provider").asText() : "parquet")
                    .build();
            
            if (formatNode.has("options")) {
                Map<String, String> options = objectMapper.convertValue(
                        formatNode.get("options"), 
                        objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class)
                );
                format.setOptions(options);
            }
            
            builder.format(format);
        }
        
        // Parse partition columns
        if (node.has("partitionColumns")) {
            List<String> partitionColumns = new ArrayList<>();
            node.get("partitionColumns").forEach(col -> partitionColumns.add(col.asText()));
            builder.partitionColumns(partitionColumns);
        }
        
        // Parse configuration
        if (node.has("configuration")) {
            Map<String, String> configuration = objectMapper.convertValue(
                    node.get("configuration"),
                    objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class)
            );
            builder.configuration(configuration);
        }
        
        return builder.build();
    }
    
    /**
     * Parse add action
     */
    private AddAction parseAddAction(JsonNode node) throws IOException {
        AddAction.AddActionBuilder builder = AddAction.builder()
                .path(node.has("path") ? node.get("path").asText() : null)
                .size(node.has("size") ? node.get("size").asLong() : null)
                .modificationTime(node.has("modificationTime") ? node.get("modificationTime").asLong() : null)
                .dataChange(node.has("dataChange") ? node.get("dataChange").asBoolean() : true);
        
        // Parse partition values
        if (node.has("partitionValues")) {
            Map<String, String> partitionValues = objectMapper.convertValue(
                    node.get("partitionValues"),
                    objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class)
            );
            builder.partitionValues(partitionValues);
        }
        
        // Parse statistics (stored as JSON string in Delta log)
        if (node.has("stats")) {
            String statsJson = node.get("stats").asText();
            builder.stats(statsJson);
            
            // Parse the stats JSON string into FileStatistics object
            if (statsJson != null && !statsJson.isEmpty()) {
                try {
                    FileStatistics parsedStats = parseStatistics(statsJson);
                    builder.parsedStats(parsedStats);
                } catch (Exception e) {
                    log.warn("Failed to parse statistics: {}", e.getMessage());
                }
            }
        }
        
        // Parse tags
        if (node.has("tags")) {
            Map<String, String> tags = objectMapper.convertValue(
                    node.get("tags"),
                    objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class)
            );
            builder.tags(tags);
        }
        
        return builder.build();
    }
    
    /**
     * Parse remove action
     */
    private RemoveAction parseRemoveAction(JsonNode node) {
        return RemoveAction.builder()
                .path(node.has("path") ? node.get("path").asText() : null)
                .deletionTimestamp(node.has("deletionTimestamp") ? node.get("deletionTimestamp").asLong() : null)
                .dataChange(node.has("dataChange") ? node.get("dataChange").asBoolean() : true)
                .build();
    }
    
    /**
     * Parse statistics JSON string
     */
    private FileStatistics parseStatistics(String statsJson) throws IOException {
        JsonNode statsNode = objectMapper.readTree(statsJson);
        
        FileStatistics.FileStatisticsBuilder builder = FileStatistics.builder();
        
        // Parse numRecords
        if (statsNode.has("numRecords")) {
            builder.numRecords(statsNode.get("numRecords").asLong());
        }
        
        // Parse minValues
        if (statsNode.has("minValues")) {
            Map<String, Object> minValues = objectMapper.convertValue(
                    statsNode.get("minValues"),
                    objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class)
            );
            builder.minValues(minValues);
        }
        
        // Parse maxValues
        if (statsNode.has("maxValues")) {
            Map<String, Object> maxValues = objectMapper.convertValue(
                    statsNode.get("maxValues"),
                    objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class)
            );
            builder.maxValues(maxValues);
        }
        
        // Parse nullCount
        if (statsNode.has("nullCount")) {
            Map<String, Long> nullCount = objectMapper.convertValue(
                    statsNode.get("nullCount"),
                    objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Long.class)
            );
            builder.nullCount(nullCount);
        }
        
        return builder.build();
    }
    
    /**
     * Find latest version in Delta log directory
     */
    private Long findLatestVersion(Path deltaLogPath) throws IOException {
        return Files.list(deltaLogPath)
                .filter(p -> p.getFileName().toString().endsWith(".json"))
                .filter(p -> !p.getFileName().toString().startsWith("_"))
                .map(p -> {
                    String fileName = p.getFileName().toString();
                    String versionStr = fileName.replace(".json", "");
                    try {
                        return Long.parseLong(versionStr);
                    } catch (NumberFormatException e) {
                        return -1L;
                    }
                })
                .filter(v -> v >= 0)
                .max(Long::compare)
                .orElse(0L);
    }
    
    /**
     * Create empty snapshot (for tables without Delta log)
     */
    private DeltaSnapshot createEmptySnapshot(Long version) {
        return DeltaSnapshot.builder()
                .version(version != null ? version : 0L)
                .addActions(new ArrayList<>())
                .build();
    }
}

