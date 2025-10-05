package com.databricks.deltasharing.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeltaSchemaDTO {
    
    private Long id;
    
    @NotBlank(message = "Name is required")
    private String name;
    
    private String description;
    
    @NotNull(message = "Share is required")
    private Long shareId;
    
    private String shareName;
    
    private LocalDateTime createdAt;
    
    private LocalDateTime updatedAt;
    
    private Integer tablesCount;
}
