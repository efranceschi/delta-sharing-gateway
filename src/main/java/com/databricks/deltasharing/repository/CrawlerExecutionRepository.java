package com.databricks.deltasharing.repository;

import com.databricks.deltasharing.model.CrawlerExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CrawlerExecutionRepository extends JpaRepository<CrawlerExecution, Long> {
    
    /**
     * Find the most recent crawler execution
     */
    Optional<CrawlerExecution> findFirstByOrderByStartedAtDesc();
    
    /**
     * Count successful executions
     */
    @Query("SELECT COUNT(e) FROM CrawlerExecution e WHERE e.status = 'SUCCESS'")
    long countSuccessfulExecutions();
    
    /**
     * Count failed executions
     */
    @Query("SELECT COUNT(e) FROM CrawlerExecution e WHERE e.status = 'FAILED'")
    long countFailedExecutions();
}

