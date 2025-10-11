package com.databricks.deltasharing.controller.api;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.search.Search;
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
    
    // Cache para armazenar histórico de requisições (última hora)
    private static final int MAX_HISTORY_SIZE = 60; // 60 pontos (1 por minuto)
    private final Map<Long, RequestStats> requestHistory = new ConcurrentHashMap<>();
    
    // Cache para armazenar estatísticas por URI
    private final Map<String, UriStats> uriStatsMap = new ConcurrentHashMap<>();
    
    public MetricsController(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
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

