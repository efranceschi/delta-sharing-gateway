package com.databricks.deltasharing.repository;

import com.databricks.deltasharing.model.DeltaShare;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DeltaShareRepository extends JpaRepository<DeltaShare, Long> {
    
    Optional<DeltaShare> findByName(String name);
    
    List<DeltaShare> findByActiveTrue();
    
    boolean existsByName(String name);
}
