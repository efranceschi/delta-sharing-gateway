package com.databricks.deltasharing.service;

import com.databricks.deltasharing.dto.DeltaSchemaDTO;
import com.databricks.deltasharing.exception.ResourceNotFoundException;
import com.databricks.deltasharing.model.DeltaSchema;
import com.databricks.deltasharing.model.DeltaShare;
import com.databricks.deltasharing.repository.DeltaSchemaRepository;
import com.databricks.deltasharing.repository.DeltaShareRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeltaSchemaManagementService {
    
    private final DeltaSchemaRepository schemaRepository;
    private final DeltaShareRepository shareRepository;
    
    @Transactional(readOnly = true)
    public List<DeltaSchemaDTO> getAllSchemas() {
        return schemaRepository.findAll().stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }
    
    @Transactional(readOnly = true)
    public DeltaSchemaDTO getSchemaById(Long id) {
        DeltaSchema schema = schemaRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Schema not found with id: " + id));
        return convertToDTO(schema);
    }
    
    @Transactional
    public DeltaSchemaDTO createSchema(DeltaSchemaDTO dto) {
        DeltaShare share = shareRepository.findById(dto.getShareId())
                .orElseThrow(() -> new ResourceNotFoundException("Share not found with id: " + dto.getShareId()));
        
        DeltaSchema schema = new DeltaSchema();
        schema.setName(dto.getName());
        schema.setDescription(dto.getDescription());
        schema.setShare(share);
        
        DeltaSchema saved = schemaRepository.save(schema);
        log.info("Created schema: {} in share: {}", saved.getName(), share.getName());
        return convertToDTO(saved);
    }
    
    @Transactional
    public DeltaSchemaDTO updateSchema(Long id, DeltaSchemaDTO dto) {
        DeltaSchema schema = schemaRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Schema not found with id: " + id));
        
        DeltaShare share = shareRepository.findById(dto.getShareId())
                .orElseThrow(() -> new ResourceNotFoundException("Share not found with id: " + dto.getShareId()));
        
        schema.setName(dto.getName());
        schema.setDescription(dto.getDescription());
        schema.setShare(share);
        
        DeltaSchema updated = schemaRepository.save(schema);
        log.info("Updated schema: {}", updated.getName());
        return convertToDTO(updated);
    }
    
    @Transactional
    public void deleteSchema(Long id) {
        if (!schemaRepository.existsById(id)) {
            throw new ResourceNotFoundException("Schema not found with id: " + id);
        }
        schemaRepository.deleteById(id);
        log.info("Deleted schema with id: {}", id);
    }
    
    private DeltaSchemaDTO convertToDTO(DeltaSchema schema) {
        return DeltaSchemaDTO.builder()
                .id(schema.getId())
                .name(schema.getName())
                .description(schema.getDescription())
                .shareId(schema.getShare().getId())
                .shareName(schema.getShare().getName())
                .createdAt(schema.getCreatedAt())
                .updatedAt(schema.getUpdatedAt())
                .tablesCount(schema.getTables() != null ? schema.getTables().size() : 0)
                .build();
    }
}
