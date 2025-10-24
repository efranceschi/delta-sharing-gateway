package com.databricks.deltasharing.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Represents a crawler execution record
 */
@Entity
@Table(name = "crawler_executions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CrawlerExecution {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "started_at", nullable = false)
    private LocalDateTime startedAt;
    
    @Column(name = "finished_at")
    private LocalDateTime finishedAt;
    
    @Column(name = "duration_ms")
    private Long durationMs;
    
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private ExecutionStatus status;
    
    @Column(name = "discovered_tables")
    @Builder.Default
    private Integer discoveredTables = 0;
    
    @Column(name = "created_schemas")
    @Builder.Default
    private Integer createdSchemas = 0;
    
    @Column(name = "created_tables")
    @Builder.Default
    private Integer createdTables = 0;
    
    @Column(name = "error_message", length = 2000)
    private String errorMessage;
    
    @Column(name = "storage_type", length = 50)
    private String storageType;
    
    @Column(name = "discovery_pattern", length = 500)
    private String discoveryPattern;
    
    @Column(name = "dry_run")
    @Builder.Default
    private Boolean dryRun = false;
    
    public enum ExecutionStatus {
        RUNNING,
        SUCCESS,
        FAILED,
        PARTIAL_SUCCESS
    }
}

