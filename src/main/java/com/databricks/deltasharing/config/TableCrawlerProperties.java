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
     */
    private boolean enabled = false;
    
    /**
     * Interval in minutes between each crawler execution
     * Default: 5 minutes
     */
    private int intervalMinutes = 5;
    
    /**
     * Discovery pattern for tables in storage
     * Default: s3://{bucket}/{schema}/{table}
     * 
     * Supported placeholders:
     * - {bucket}: Storage bucket/container name
     * - {share}: Share name
     * - {schema}: Schema name
     * - {table}: Table name
     * 
     * Examples:
     * - s3://{bucket}/{schema}/{table}
     * - s3://{bucket}/{share}/{schema}/{table}
     * - /data/{share}/{schema}/{table}
     */
    private String discoveryPattern = "s3://{bucket}/{schema}/{table}";
    
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

