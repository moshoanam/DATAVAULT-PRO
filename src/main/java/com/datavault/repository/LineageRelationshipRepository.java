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
public interface LineageRelationshipRepository extends JpaRepository<LineageRelationship, Long> {
    List<LineageRelationship> findBySourceField(FieldMetadata sourceField);
    List<LineageRelationship> findByTargetField(FieldMetadata targetField);

    default List<LineageRelationship> findUpstreamLineage(FieldMetadata field) {
        return findByTargetField(field);
    }

    default List<LineageRelationship> findDownstreamLineage(FieldMetadata field) {
        return findBySourceField(field);
    }
    
    @Query("SELECT l FROM LineageRelationship l WHERE " +
           "l.sourceField.id = :fieldId OR l.targetField.id = :fieldId")
    List<LineageRelationship> findAllByField(@Param("fieldId") Long fieldId);
    
    @Query("SELECT COUNT(l) FROM LineageRelationship l WHERE l.targetField.id = :fieldId")
    long countDownstreamDependencies(@Param("fieldId") Long fieldId);
    
    @Query("SELECT COUNT(l) FROM LineageRelationship l WHERE l.sourceField.id = :fieldId")
    long countUpstreamDependencies(@Param("fieldId") Long fieldId);
}
