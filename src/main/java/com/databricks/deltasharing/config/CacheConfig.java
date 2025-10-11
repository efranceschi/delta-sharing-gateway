package com.databricks.deltasharing.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCache;
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
     * - "databaseHealthCheck": Caches Database health check results (TTL: 60 seconds)
     * - "jvmHealthCheck": Caches JVM memory metrics (TTL: 10 seconds)
     * - "minioClusterHealthCheck": Caches MinIO cluster info (TTL: 60 seconds)
     * 
     * @return Configured CacheManager
     */
    @Bean
    public CacheManager cacheManager(MeterRegistry meterRegistry) {
        log.info("Initializing Cache Manager:");
        log.info("  - Schema Cache TTL: {} minutes", schemaCacheTtlMinutes);
        log.info("  - Schema Cache Max Size: {} entries", schemaCacheMaxSize);
        log.info("  - Health Check Cache TTL: 60 seconds");
        log.info("  - JVM Cache TTL: 10 seconds");
        
        CaffeineCacheManager cacheManager = new CaffeineCacheManager(
                "tableSchemas", 
                "partitionColumns"
        );
        
        cacheManager.setCaffeine(Caffeine.newBuilder()
                .expireAfterWrite(schemaCacheTtlMinutes, TimeUnit.MINUTES)
                .maximumSize(schemaCacheMaxSize)
                .recordStats() // Enable statistics for monitoring
        );
        
        // Configure health check caches with 60 second TTL
        cacheManager.registerCustomCache("minioHealthCheck", 
                Caffeine.newBuilder()
                        .expireAfterWrite(60, TimeUnit.SECONDS)
                        .maximumSize(1)
                        .recordStats()
                        .build()
        );
        
        cacheManager.registerCustomCache("databaseHealthCheck", 
                Caffeine.newBuilder()
                        .expireAfterWrite(60, TimeUnit.SECONDS)
                        .maximumSize(1)
                        .recordStats()
                        .build()
        );
        
        cacheManager.registerCustomCache("minioClusterHealthCheck", 
                Caffeine.newBuilder()
                        .expireAfterWrite(60, TimeUnit.SECONDS)
                        .maximumSize(1)
                        .recordStats()
                        .build()
        );
        
        // Configure JVM cache with shorter TTL (10 seconds)
        cacheManager.registerCustomCache("jvmHealthCheck", 
                Caffeine.newBuilder()
                        .expireAfterWrite(10, TimeUnit.SECONDS)
                        .maximumSize(1)
                        .recordStats()
                        .build()
        );
        
        log.info("Cache Manager initialized successfully");
        
        // Register metrics for default caches
        registerCacheMetrics(cacheManager, "tableSchemas", meterRegistry);
        registerCacheMetrics(cacheManager, "partitionColumns", meterRegistry);
        registerCacheMetrics(cacheManager, "minioHealthCheck", meterRegistry);
        registerCacheMetrics(cacheManager, "databaseHealthCheck", meterRegistry);
        registerCacheMetrics(cacheManager, "jvmHealthCheck", meterRegistry);
        registerCacheMetrics(cacheManager, "minioClusterHealthCheck", meterRegistry);
        
        return cacheManager;
    }
    
    /**
     * Register cache metrics with Micrometer
     */
    private void registerCacheMetrics(CacheManager cacheManager, String cacheName, MeterRegistry meterRegistry) {
        org.springframework.cache.Cache cache = cacheManager.getCache(cacheName);
        if (cache instanceof CaffeineCache) {
            CaffeineCache caffeineCache = (CaffeineCache) cache;
            com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = caffeineCache.getNativeCache();
            CaffeineCacheMetrics.monitor(meterRegistry, nativeCache, cacheName);
            log.info("  - Cache '{}' registered with Micrometer", cacheName);
        }
    }
}

