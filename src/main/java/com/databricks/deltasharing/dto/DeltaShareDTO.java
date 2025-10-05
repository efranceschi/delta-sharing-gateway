package com.databricks.deltasharing.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeltaShareDTO {
    
    private Long id;
    
    @NotBlank(message = "Name is required")
    private String name;
    
    private String description;
    
    private Boolean active;
    
    private LocalDateTime createdAt;
    
    private LocalDateTime updatedAt;
    
    private Integer schemasCount;
}
