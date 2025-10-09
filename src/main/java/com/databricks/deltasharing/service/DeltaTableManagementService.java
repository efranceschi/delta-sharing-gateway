package com.databricks.deltasharing.service;

import com.databricks.deltasharing.dto.DeltaTableDTO;
import com.databricks.deltasharing.exception.ResourceNotFoundException;
import com.databricks.deltasharing.model.DeltaSchema;
import com.databricks.deltasharing.model.DeltaTable;
import com.databricks.deltasharing.repository.DeltaSchemaRepository;
import com.databricks.deltasharing.repository.DeltaTableRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeltaTableManagementService {
    
    private final DeltaTableRepository tableRepository;
    private final DeltaSchemaRepository schemaRepository;
    
    @Transactional(readOnly = true)
    public List<DeltaTableDTO> getAllTables() {
        return tableRepository.findAll().stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }
    
    @Transactional(readOnly = true)
    public DeltaTableDTO getTableById(Long id) {
        DeltaTable table = tableRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Table not found with id: " + id));
        return convertToDTO(table);
    }
    
    @Transactional(readOnly = true)
    public List<DeltaTableDTO> getTablesBySchemaId(Long schemaId) {
        return tableRepository.findBySchemaId(schemaId).stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }
    
    @Transactional
    public DeltaTableDTO createTable(DeltaTableDTO dto) {
        DeltaSchema schema = schemaRepository.findById(dto.getSchemaId())
                .orElseThrow(() -> new ResourceNotFoundException("Schema not found with id: " + dto.getSchemaId()));
        
        DeltaTable table = new DeltaTable();
        table.setName(dto.getName());
        table.setDescription(dto.getDescription());
        table.setSchema(schema);
        table.setShareAsView(dto.getShareAsView() != null ? dto.getShareAsView() : false);
        table.setLocation(dto.getLocation());
        table.setFormat(dto.getFormat() != null ? dto.getFormat() : "parquet");
        
        DeltaTable saved = tableRepository.save(table);
        log.info("Created table: {} in schema: {}", saved.getName(), schema.getName());
        return convertToDTO(saved);
    }
    
    @Transactional
    public DeltaTableDTO updateTable(Long id, DeltaTableDTO dto) {
        DeltaTable table = tableRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Table not found with id: " + id));
        
        DeltaSchema schema = schemaRepository.findById(dto.getSchemaId())
                .orElseThrow(() -> new ResourceNotFoundException("Schema not found with id: " + dto.getSchemaId()));
        
        table.setName(dto.getName());
        table.setDescription(dto.getDescription());
        table.setSchema(schema);
        table.setShareAsView(dto.getShareAsView());
        table.setLocation(dto.getLocation());
        table.setFormat(dto.getFormat());
        
        DeltaTable updated = tableRepository.save(table);
        log.info("Updated table: {}", updated.getName());
        return convertToDTO(updated);
    }
    
    @Transactional
    public void deleteTable(Long id) {
        if (!tableRepository.existsById(id)) {
            throw new ResourceNotFoundException("Table not found with id: " + id);
        }
        tableRepository.deleteById(id);
        log.info("Deleted table with id: {}", id);
    }
    
    private DeltaTableDTO convertToDTO(DeltaTable table) {
        return DeltaTableDTO.builder()
                .id(table.getId())
                .name(table.getName())
                .description(table.getDescription())
                .schemaId(table.getSchema().getId())
                .schemaName(table.getSchema().getName())
                .shareId(table.getSchema().getShare().getId())
                .shareName(table.getSchema().getShare().getName())
                .shareAsView(table.getShareAsView())
                .location(table.getLocation())
                .format(table.getFormat())
                .createdAt(table.getCreatedAt())
                .updatedAt(table.getUpdatedAt())
                .build();
    }
}
