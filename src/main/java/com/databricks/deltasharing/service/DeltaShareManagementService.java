package com.databricks.deltasharing.service;

import com.databricks.deltasharing.dto.DeltaShareDTO;
import com.databricks.deltasharing.exception.DuplicateResourceException;
import com.databricks.deltasharing.exception.ResourceNotFoundException;
import com.databricks.deltasharing.model.DeltaShare;
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
public class DeltaShareManagementService {
    
    private final DeltaShareRepository shareRepository;
    
    @Transactional(readOnly = true)
    public List<DeltaShareDTO> getAllShares() {
        return shareRepository.findAll().stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }
    
    @Transactional(readOnly = true)
    public DeltaShareDTO getShareById(Long id) {
        DeltaShare share = shareRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Share not found with id: " + id));
        return convertToDTO(share);
    }
    
    @Transactional
    public DeltaShareDTO createShare(DeltaShareDTO dto) {
        if (shareRepository.existsByName(dto.getName())) {
            throw new DuplicateResourceException("Share already exists with name: " + dto.getName());
        }
        
        DeltaShare share = new DeltaShare();
        share.setName(dto.getName());
        share.setDescription(dto.getDescription());
        share.setActive(dto.getActive() != null ? dto.getActive() : true);
        
        DeltaShare saved = shareRepository.save(share);
        log.info("Created share: {}", saved.getName());
        return convertToDTO(saved);
    }
    
    @Transactional
    public DeltaShareDTO updateShare(Long id, DeltaShareDTO dto) {
        DeltaShare share = shareRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Share not found with id: " + id));
        
        if (!share.getName().equals(dto.getName()) && shareRepository.existsByName(dto.getName())) {
            throw new DuplicateResourceException("Share already exists with name: " + dto.getName());
        }
        
        share.setName(dto.getName());
        share.setDescription(dto.getDescription());
        share.setActive(dto.getActive());
        
        DeltaShare updated = shareRepository.save(share);
        log.info("Updated share: {}", updated.getName());
        return convertToDTO(updated);
    }
    
    @Transactional
    public void deleteShare(Long id) {
        if (!shareRepository.existsById(id)) {
            throw new ResourceNotFoundException("Share not found with id: " + id);
        }
        shareRepository.deleteById(id);
        log.info("Deleted share with id: {}", id);
    }
    
    private DeltaShareDTO convertToDTO(DeltaShare share) {
        return DeltaShareDTO.builder()
                .id(share.getId())
                .name(share.getName())
                .description(share.getDescription())
                .active(share.getActive())
                .createdAt(share.getCreatedAt())
                .updatedAt(share.getUpdatedAt())
                .schemasCount(share.getSchemas() != null ? share.getSchemas().size() : 0)
                .build();
    }
}
