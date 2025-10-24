package com.databricks.deltasharing.service;

import com.databricks.deltasharing.model.CrawlerExecution;
import com.databricks.deltasharing.repository.CrawlerExecutionRepository;
import com.databricks.deltasharing.repository.DeltaTableRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Service to provide crawler status information
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CrawlerStatusService {
    
    private final CrawlerExecutionRepository executionRepository;
    private final DeltaTableRepository tableRepository;
    
    /**
     * Get the last crawler execution
     */
    public Optional<CrawlerExecution> getLastExecution() {
        return executionRepository.findFirstByOrderByStartedAtDesc();
    }
    
    /**
     * Get total number of auto-discovered tables
     */
    public long countAutoDiscoveredTables() {
        return tableRepository.findAll().stream()
                .filter(table -> table.getDiscoveredAt() != null && "crawler".equals(table.getDiscoveredBy()))
                .count();
    }
    
    /**
     * Get total number of manually created tables
     */
    public long countManualTables() {
        return tableRepository.findAll().stream()
                .filter(table -> table.getDiscoveredAt() == null)
                .count();
    }
    
    /**
     * Get success rate percentage
     */
    public double getSuccessRate() {
        long total = executionRepository.count();
        if (total == 0) {
            return 0.0;
        }
        long successful = executionRepository.countSuccessfulExecutions();
        return (successful * 100.0) / total;
    }
}

