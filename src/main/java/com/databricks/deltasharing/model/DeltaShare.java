package com.databricks.deltasharing.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import lombok.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Represents a Delta Share - a logical grouping of schemas
 * Based on Delta Sharing Protocol specification
 */
@Entity
@Table(name = "delta_shares")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString(exclude = "schemas")
@EqualsAndHashCode(exclude = "schemas")
public class DeltaShare {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false, unique = true, length = 36)
    private String uuid;
    
    @NotBlank(message = "Share name is required")
    @Column(nullable = false, unique = true)
    private String name;
    
    @Column(length = 2000)
    private String description;
    
    @Column(nullable = false)
    private Boolean active = true;
    
    @OneToMany(mappedBy = "share", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<DeltaSchema> schemas = new ArrayList<>();
    
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    @PrePersist
    protected void onCreate() {
        if (uuid == null || uuid.isEmpty()) {
            uuid = UUID.randomUUID().toString();
        }
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }
    
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
