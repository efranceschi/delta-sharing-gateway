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
 * - Health checks: Cached to avoid excessive external service calls
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
     * - "tableSchemas": Caches table schema JSON strings (TTL: configured minutes)
     * - "partitionColumns": Caches partition column arrays (TTL: configured minutes)
     * - "minioHealthCheck": Caches MinIO health check results (TTL: 60 seconds)
     * 
     * @return Configured CacheManager
     */
    @Bean
    public CacheManager cacheManager() {
        log.info("Initializing Cache Manager:");
        log.info("  - Schema Cache TTL: {} minutes", schemaCacheTtlMinutes);
        log.info("  - Schema Cache Max Size: {} entries", schemaCacheMaxSize);
        log.info("  - Health Check Cache TTL: 60 seconds");
        
        CaffeineCacheManager cacheManager = new CaffeineCacheManager(
                "tableSchemas", 
                "partitionColumns", 
                "minioHealthCheck"
        );
        
        cacheManager.setCaffeine(Caffeine.newBuilder()
                .expireAfterWrite(schemaCacheTtlMinutes, TimeUnit.MINUTES)
                .maximumSize(schemaCacheMaxSize)
                .recordStats() // Enable statistics for monitoring
        );
        
        // Configure a separate cache for health checks with shorter TTL
        cacheManager.registerCustomCache("minioHealthCheck", 
                Caffeine.newBuilder()
                        .expireAfterWrite(60, TimeUnit.SECONDS)
                        .maximumSize(1)
                        .recordStats()
                        .build()
        );
        
        log.info("Cache Manager initialized successfully");
        return cacheManager;
    }
}

