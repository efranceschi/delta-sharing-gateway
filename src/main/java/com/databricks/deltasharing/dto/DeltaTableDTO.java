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
public class DeltaTableDTO {
    
    private Long id;
    
    @NotBlank(message = "Name is required")
    private String name;
    
    private String description;
    
    @NotNull(message = "Schema is required")
    private Long schemaId;
    
    private String schemaName;
    
    private String shareName;
    
    private Boolean shareAsView;
    
    private String location;
    
    private String format;
    
    private LocalDateTime createdAt;
    
    private LocalDateTime updatedAt;
}
