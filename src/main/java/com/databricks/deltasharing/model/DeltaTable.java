package com.databricks.deltasharing.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Represents a Delta Table - a table within a schema that can be shared
 * Based on Delta Sharing Protocol specification
 */
@Entity
@Table(name = "delta_tables", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"name", "schema_id"})
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString(exclude = "schema")
@EqualsAndHashCode(exclude = "schema")
public class DeltaTable {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @NotBlank(message = "Table name is required")
    @Column(nullable = false)
    private String name;
    
    @Column(length = 2000)
    private String description;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "schema_id", nullable = false)
    private DeltaSchema schema;
    
    @Column(name = "share_as_view")
    private Boolean shareAsView = false;
    
    @Column(name = "location", length = 1000)
    private String location;
    
    @Column(name = "table_format")
    private String format = "parquet";
    
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
