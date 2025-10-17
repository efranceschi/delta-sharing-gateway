package com.databricks.deltasharing.controller.api;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.search.Search;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/metrics")
@EnableScheduling
public class MetricsController {

    private final MeterRegistry meterRegistry;
    private final CacheManager cacheManager;
    
    // Cache para armazenar histórico de requisições (última hora)
    private static final int MAX_HISTORY_SIZE = 60; // 60 pontos (1 por minuto)
    private final Map<Long, RequestStats> requestHistory = new ConcurrentHashMap<>();
    
    // Cache para armazenar estatísticas por URI
    private final Map<String, UriStats> uriStatsMap = new ConcurrentHashMap<>();
    
    public MetricsController(MeterRegistry meterRegistry, CacheManager cacheManager) {
        this.meterRegistry = meterRegistry;
        this.cacheManager = cacheManager;
    }
    
    @PostConstruct
    public void init() {
        // Coletar métricas iniciais
        collectMetrics();
    }
    
    // Coletar métricas a cada 60 segundos
    @Scheduled(fixedRate = 60000)
    public void scheduledCollectMetrics() {
        collectMetrics();
    }
    
    @GetMapping("/http")
    public Map<String, Object> getHttpMetrics() {
        Map<String, Object> result = new HashMap<>();
        
        result.put("byStatus", getRequestsByStatus());
        result.put("topUrls", getTopAccessedUrls(10));
        result.put("slowestUrls", getSlowestUrls(10));
        result.put("timeline", getRequestTimeline());
        result.put("cache", getCacheMetrics());
        
        return result;
    }
    
    @GetMapping("/cache")
    public Map<String, Object> getCacheDetails() {
        Map<String, Object> result = new HashMap<>();
        
        // Lista de caches a serem monitorados
        String[] cacheNames = {
            "tableSchemas", 
            "partitionColumns", 
            "minioHealthCheck", 
            "databaseHealthCheck", 
            "jvmHealthCheck", 
            "minioClusterHealthCheck"
        };
        
        List<Map<String, Object>> cacheList = new ArrayList<>();
        long totalHits = 0;
        long totalMisses = 0;
        
        for (String cacheName : cacheNames) {
            try {
                org.springframework.cache.Cache cache = cacheManager.getCache(cacheName);
                if (cache instanceof CaffeineCache) {
                    CaffeineCache caffeineCache = (CaffeineCache) cache;
                    com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = caffeineCache.getNativeCache();
                    CacheStats stats = nativeCache.stats();
                    
                    Map<String, Object> cacheInfo = new HashMap<>();
                    cacheInfo.put("name", cacheName);
                    cacheInfo.put("hits", stats.hitCount());
                    cacheInfo.put("misses", stats.missCount());
                    cacheInfo.put("total", stats.requestCount());
                    cacheInfo.put("hitRate", stats.hitRate() * 100.0);
                    cacheInfo.put("size", nativeCache.estimatedSize());
                    cacheInfo.put("evictions", stats.evictionCount());
                    cacheInfo.put("loadSuccess", stats.loadSuccessCount());
                    cacheInfo.put("loadFailure", stats.loadFailureCount());
                    cacheInfo.put("averageLoadTime", stats.averageLoadPenalty() / 1_000_000.0); // Convert to ms
                    
                    cacheList.add(cacheInfo);
                    
                    totalHits += stats.hitCount();
                    totalMisses += stats.missCount();
                }
            } catch (Exception e) {
                // Cache pode não existir, ignorar
            }
        }
        
        long grandTotal = totalHits + totalMisses;
        double grandHitRate = grandTotal > 0 ? (totalHits * 100.0 / grandTotal) : 0.0;
        
        Map<String, Object> summary = new HashMap<>();
        summary.put("totalHits", totalHits);
        summary.put("totalMisses", totalMisses);
        summary.put("total", grandTotal);
        summary.put("hitRate", grandHitRate);
        
        result.put("summary", summary);
        result.put("caches", cacheList);
        
        return result;
    }
    
    private Map<String, Object> getCacheMetrics() {
        Map<String, Object> result = new HashMap<>();
        
        long totalHits = 0;
        long totalMisses = 0;
        
        // Lista de caches a serem monitorados
        String[] cacheNames = {
            "tableSchemas", 
            "partitionColumns", 
            "minioHealthCheck", 
            "databaseHealthCheck", 
            "jvmHealthCheck", 
            "minioClusterHealthCheck"
        };
        
        // Coletar estatísticas de cada cache Caffeine
        for (String cacheName : cacheNames) {
            try {
                org.springframework.cache.Cache cache = cacheManager.getCache(cacheName);
                if (cache instanceof CaffeineCache) {
                    CaffeineCache caffeineCache = (CaffeineCache) cache;
                    com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = caffeineCache.getNativeCache();
                    CacheStats stats = nativeCache.stats();
                    
                    totalHits += stats.hitCount();
                    totalMisses += stats.missCount();
                }
            } catch (Exception e) {
                // Cache pode não existir, ignorar
            }
        }
        
        long total = totalHits + totalMisses;
        double hitRate = total > 0 ? (totalHits * 100.0 / total) : 0.0;
        
        result.put("hits", totalHits);
        result.put("misses", totalMisses);
        result.put("total", total);
        result.put("hitRate", hitRate);
        
        return result;
    }
    
    private Map<String, Long> getRequestsByStatus() {
        Map<String, Long> statusCounts = new LinkedHashMap<>();
        
        Search.in(meterRegistry)
            .name("http.server.requests")
            .meters()
            .forEach(meter -> {
                if (meter instanceof io.micrometer.core.instrument.Timer) {
                    io.micrometer.core.instrument.Timer timer = (io.micrometer.core.instrument.Timer) meter;
                    String status = timer.getId().getTag("status");
                    if (status != null) {
                        long count = timer.count();
                        statusCounts.merge(status, count, Long::sum);
                    }
                }
            });
        
        return statusCounts.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (e1, e2) -> e1,
                LinkedHashMap::new
            ));
    }
    
    private List<Map<String, Object>> getTopAccessedUrls(int limit) {
        return uriStatsMap.values().stream()
            .sorted(Comparator.comparingLong(UriStats::getCount).reversed())
            .limit(limit)
            .map(stats -> {
                Map<String, Object> map = new HashMap<>();
                map.put("uri", stats.uri);
                map.put("count", stats.count);
                map.put("avgTime", stats.getAverageTime());
                map.put("totalTime", stats.totalTime);
                return map;
            })
            .collect(Collectors.toList());
    }
    
    private List<Map<String, Object>> getSlowestUrls(int limit) {
        return uriStatsMap.values().stream()
            .filter(stats -> stats.count > 0)
            .sorted(Comparator.comparingDouble(UriStats::getAverageTime).reversed())
            .limit(limit)
            .map(stats -> {
                Map<String, Object> map = new HashMap<>();
                map.put("uri", stats.uri);
                map.put("count", stats.count);
                map.put("avgTime", stats.getAverageTime());
                map.put("totalTime", stats.totalTime);
                return map;
            })
            .collect(Collectors.toList());
    }
    
    private Map<String, Object> getRequestTimeline() {
        long now = System.currentTimeMillis();
        long oneHourAgo = now - TimeUnit.HOURS.toMillis(1);
        
        // Limpar entradas antigas
        requestHistory.entrySet().removeIf(entry -> entry.getKey() < oneHourAgo);
        
        List<Long> timestamps = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        List<Double> avgTimes = new ArrayList<>();
        
        requestHistory.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> {
                timestamps.add(entry.getKey());
                counts.add(entry.getValue().count);
                avgTimes.add(entry.getValue().avgTime);
            });
        
        Map<String, Object> timeline = new HashMap<>();
        timeline.put("timestamps", timestamps);
        timeline.put("counts", counts);
        timeline.put("avgTimes", avgTimes);
        
        return timeline;
    }
    
    private void collectMetrics() {
        long currentMinute = System.currentTimeMillis() / 60000 * 60000;
        
        long totalCount = 0;
        double totalTime = 0.0;
        
        // Coletar métricas de todos os timers HTTP
        Search.in(meterRegistry)
            .name("http.server.requests")
            .meters()
            .forEach(meter -> {
                if (meter instanceof io.micrometer.core.instrument.Timer) {
                    io.micrometer.core.instrument.Timer timer = (io.micrometer.core.instrument.Timer) meter;
                    String uri = timer.getId().getTag("uri");
                    
                    if (uri != null && !uri.startsWith("/actuator")) {
                        long count = timer.count();
                        double totalTimeNanos = timer.totalTime(TimeUnit.NANOSECONDS);
                        double totalTimeMs = totalTimeNanos / 1_000_000.0;
                        
                        // Atualizar estatísticas por URI
                        uriStatsMap.computeIfAbsent(uri, k -> new UriStats(uri))
                            .update(count, totalTimeMs);
                    }
                }
            });
        
        // Calcular totais atuais
        for (UriStats stats : uriStatsMap.values()) {
            totalCount += stats.count;
            totalTime += stats.totalTime;
        }
        
        // Armazenar no histórico
        double avgTime = totalCount > 0 ? totalTime / totalCount : 0.0;
        requestHistory.put(currentMinute, new RequestStats(totalCount, avgTime));
        
        // Limitar tamanho do histórico
        if (requestHistory.size() > MAX_HISTORY_SIZE) {
            requestHistory.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .limit(requestHistory.size() - MAX_HISTORY_SIZE)
                .map(Map.Entry::getKey)
                .forEach(requestHistory::remove);
        }
    }
    
    private static class RequestStats {
        long count;
        double avgTime;
        
        RequestStats(long count, double avgTime) {
            this.count = count;
            this.avgTime = avgTime;
        }
    }
    
    private static class UriStats {
        String uri;
        long count;
        double totalTime;
        
        UriStats(String uri) {
            this.uri = uri;
            this.count = 0;
            this.totalTime = 0.0;
        }
        
        void update(long newCount, double newTotalTime) {
            this.count = newCount;
            this.totalTime = newTotalTime;
        }
        
        double getAverageTime() {
            return count > 0 ? totalTime / count : 0.0;
        }
        
        long getCount() {
            return count;
        }
    }
}

