package com.databricks.deltasharing.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Cache Configuration for Delta Sharing Gateway
 * 
 * Provides high-performance caching for frequently accessed data:
 * - Table schemas: Cached to avoid repeated Delta Log reads
 * - Partition columns: Cached for quick metadata retrieval
 * 
 * Uses Caffeine cache with configurable TTL and maximum size.
 */
@Slf4j
@Configuration
@EnableCaching
public class CacheConfig {

    @Value("${delta.sharing.cache.schema.ttl-minutes:60}")
    private long schemaCacheTtlMinutes;

    @Value("${delta.sharing.cache.schema.max-size:1000}")
    private long schemaCacheMaxSize;

    /**
     * Cache Manager bean with Caffeine implementation
     * 
     * Cache Names:
     * - "tableSchemas": Caches table schema JSON strings
     * - "partitionColumns": Caches partition column arrays
     * 
     * @return Configured CacheManager
     */
    @Bean
    public CacheManager cacheManager() {
        log.info("Initializing Cache Manager:");
        log.info("  - Schema Cache TTL: {} minutes", schemaCacheTtlMinutes);
        log.info("  - Schema Cache Max Size: {} entries", schemaCacheMaxSize);
        
        CaffeineCacheManager cacheManager = new CaffeineCacheManager("tableSchemas", "partitionColumns");
        
        cacheManager.setCaffeine(Caffeine.newBuilder()
                .expireAfterWrite(schemaCacheTtlMinutes, TimeUnit.MINUTES)
                .maximumSize(schemaCacheMaxSize)
                .recordStats() // Enable statistics for monitoring
        );
        
        log.info("Cache Manager initialized successfully");
        return cacheManager;
    }
}

