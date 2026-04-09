package com.datavault.repository;

import com.datavault.entity.*;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface FieldMetadataRepository extends JpaRepository<FieldMetadata, Long> {
    List<FieldMetadata> findByTable(TableMetadata table);
    List<FieldMetadata> findByTableId(Long tableId);
    Optional<FieldMetadata> findByTableAndFieldName(TableMetadata table, String fieldName);
    List<FieldMetadata> findBySensitivityLevel(SensitivityLevel level);
    Page<FieldMetadata> findBySensitivityLevel(SensitivityLevel level, Pageable pageable);
    
    List<FieldMetadata> findByGlossaryTerm(GlossaryTerm term);
    
    @Query("SELECT f FROM FieldMetadata f WHERE f.table.id = :tableId " +
           "AND f.sensitivityLevel = :sensitivity")
    List<FieldMetadata> findByTableAndSensitivity(
        @Param("tableId") Long tableId,
        @Param("sensitivity") SensitivityLevel sensitivity
    );
    
    @Query("SELECT f FROM FieldMetadata f WHERE " +
           "LOWER(f.fieldName) LIKE LOWER(CONCAT('%', :query, '%')) OR " +
           "LOWER(f.businessName) LIKE LOWER(CONCAT('%', :query, '%')) OR " +
           "LOWER(f.description) LIKE LOWER(CONCAT('%', :query, '%'))")
    Page<FieldMetadata> searchFields(@Param("query") String query, Pageable pageable);
    
    long countBySensitivityLevel(SensitivityLevel level);
    long countByOwnerEmailNotNull();
    long countByDescriptionNotNull();
    
    List<FieldMetadata> findByOwnerEmail(String ownerEmail);
}
