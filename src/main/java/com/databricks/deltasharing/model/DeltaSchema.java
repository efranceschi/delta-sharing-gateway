package com.databricks.deltasharing.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a Delta Schema - a logical grouping of tables within a share
 * Based on Delta Sharing Protocol specification
 */
@Entity
@Table(name = "delta_schemas", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"name", "share_id"})
})
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeltaSchema {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @NotBlank(message = "Schema name is required")
    @Column(nullable = false)
    private String name;
    
    @Column(length = 2000)
    private String description;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "share_id", nullable = false)
    private DeltaShare share;
    
    @OneToMany(mappedBy = "schema", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<DeltaTable> tables = new ArrayList<>();
    
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }
    
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
