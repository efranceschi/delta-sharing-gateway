package com.databricks.deltasharing.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for the automatic table crawler
 */
@Configuration
@ConfigurationProperties(prefix = "delta.sharing.crawler")
@Data
public class TableCrawlerProperties {
    
    /**
     * Enable or disable the automatic table crawler
     * Default: true (enabled)
     */
    private boolean enabled = true;
    
    /**
     * Interval in minutes between each crawler execution
     * Default: 5 minutes
     */
    private int intervalMinutes = 5;
    
    /**
     * Initial delay before first crawler execution (in minutes)
     * Default: 1 minute
     */
    private int initialDelayMinutes = 1;
    
    /**
     * Discovery pattern for tables in storage
     * Default: s3://{share}/{schema}/{table}
     * 
     * Note: The {share} placeholder is used as the S3/MinIO bucket name
     * 
     * Supported placeholders:
     * - {share}: Share name (also used as bucket/container name in S3/MinIO)
     * - {schema}: Schema name
     * - {table}: Table name
     * 
     * Examples:
     * - s3://{share}/{schema}/{table}     (share name = bucket name)
     * - /data/{share}/{schema}/{table}    (for filesystem storage)
     * - http://host/{share}/{schema}/{table}  (for HTTP storage)
     */
    private String discoveryPattern = "s3://{share}/{schema}/{table}";
    
    /**
     * Automatically create missing schemas when discovering tables
     */
    private boolean autoCreateSchemas = true;
    
    /**
     * Dry-run mode: log discoveries without creating tables
     */
    private boolean dryRun = false;
    
    /**
     * File formats to scan for (comma-separated)
     * Default: delta,parquet
     */
    private String scanFormats = "delta,parquet";
    
    /**
     * Maximum depth to scan in directory structure
     * Default: 5
     */
    private int maxScanDepth = 5;
}

