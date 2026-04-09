package com.datavault.service;

import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.exception.ResourceNotFoundException;
import com.datavault.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

// ========== Lineage Service (End-to-End Field-Level Lineage) ==========
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class LineageService {
    
    private final LineageRelationshipRepository lineageRepository;
    private final FieldMetadataRepository fieldRepository;
    private final ChangeHistoryRepository changeHistoryRepository;

    public LineageGraphDTO getUpstreamLineage(Long fieldId, int maxDepth) {
        log.info("Getting upstream lineage for field: {} with depth: {}", fieldId, maxDepth);
        
        FieldMetadata field = fieldRepository.findById(fieldId)
            .orElseThrow(() -> new ResourceNotFoundException("Field not found"));
        
        Set<LineageNodeDTO> nodes = new HashSet<>();
        Set<LineageEdgeDTO> edges = new HashSet<>();
        Set<Long> visited = new HashSet<>();
        
        traverseUpstream(field, nodes, edges, visited, 0, maxDepth);
        
        return LineageGraphDTO.builder()
            .rootFieldId(fieldId)
            .rootFieldName(field.getFieldName())
            .direction("UPSTREAM")
            .nodes(new ArrayList<>(nodes))
            .edges(new ArrayList<>(edges))
            .depth(maxDepth)
            .build();
    }

    public LineageGraphDTO getDownstreamLineage(Long fieldId, int maxDepth) {
        log.info("Getting downstream lineage for field: {} with depth: {}", fieldId, maxDepth);
        
        FieldMetadata field = fieldRepository.findById(fieldId)
            .orElseThrow(() -> new ResourceNotFoundException("Field not found"));
        
        Set<LineageNodeDTO> nodes = new HashSet<>();
        Set<LineageEdgeDTO> edges = new HashSet<>();
        Set<Long> visited = new HashSet<>();
        
        traverseDownstream(field, nodes, edges, visited, 0, maxDepth);
        
        return LineageGraphDTO.builder()
            .rootFieldId(fieldId)
            .rootFieldName(field.getFieldName())
            .direction("DOWNSTREAM")
            .nodes(new ArrayList<>(nodes))
            .edges(new ArrayList<>(edges))
            .depth(maxDepth)
            .build();
    }

    public LineageGraphDTO getCompleteLineage(Long fieldId, int maxDepth) {
        LineageGraphDTO upstream = getUpstreamLineage(fieldId, maxDepth);
        LineageGraphDTO downstream = getDownstreamLineage(fieldId, maxDepth);
        
        Set<LineageNodeDTO> allNodes = new HashSet<>(upstream.getNodes());
        allNodes.addAll(downstream.getNodes());
        
        Set<LineageEdgeDTO> allEdges = new HashSet<>(upstream.getEdges());
        allEdges.addAll(downstream.getEdges());
        
        return LineageGraphDTO.builder()
            .rootFieldId(fieldId)
            .rootFieldName(upstream.getRootFieldName())
            .direction("COMPLETE")
            .nodes(new ArrayList<>(allNodes))
            .edges(new ArrayList<>(allEdges))
            .depth(maxDepth)
            .build();
    }

    private void traverseUpstream(FieldMetadata field, Set<LineageNodeDTO> nodes, 
                                  Set<LineageEdgeDTO> edges, Set<Long> visited, 
                                  int currentDepth, int maxDepth) {
        if (currentDepth > maxDepth || visited.contains(field.getId())) {
            return;
        }
        
        visited.add(field.getId());
        nodes.add(createNodeDTO(field, currentDepth));
        
        List<LineageRelationship> upstreamRels = lineageRepository
            .findByTargetField(field);
        
        for (LineageRelationship rel : upstreamRels) {
            edges.add(createEdgeDTO(rel));
            traverseUpstream(rel.getSourceField(), nodes, edges, visited, 
                           currentDepth + 1, maxDepth);
        }
    }

    private void traverseDownstream(FieldMetadata field, Set<LineageNodeDTO> nodes, 
                                   Set<LineageEdgeDTO> edges, Set<Long> visited, 
                                   int currentDepth, int maxDepth) {
        if (currentDepth > maxDepth || visited.contains(field.getId())) {
            return;
        }
        
        visited.add(field.getId());
        nodes.add(createNodeDTO(field, currentDepth));
        
        List<LineageRelationship> downstreamRels = lineageRepository
            .findBySourceField(field);
        
        for (LineageRelationship rel : downstreamRels) {
            edges.add(createEdgeDTO(rel));
            traverseDownstream(rel.getTargetField(), nodes, edges, visited, 
                             currentDepth + 1, maxDepth);
        }
    }

    public ImpactAnalysisDTO analyzeImpact(ImpactAnalysisRequestDTO request) {
        log.info("Analyzing impact for field: {}", request.getFieldId());
        
        LineageGraphDTO downstream = getDownstreamLineage(request.getFieldId(), 10);
        
        Set<String> affectedTables = downstream.getNodes().stream()
            .map(LineageNodeDTO::getTableName)
            .collect(Collectors.toSet());
        
        Set<String> affectedSystems = downstream.getNodes().stream()
            .map(LineageNodeDTO::getDatabaseName)
            .collect(Collectors.toSet());
        
        List<BreakingChangeDTO> breakingChanges = identifyBreakingChanges(downstream, request);
        
        return ImpactAnalysisDTO.builder()
            .fieldId(request.getFieldId())
            .changeType(request.getChangeType())
            .totalDownstreamDependencies(downstream.getNodes().size())
            .affectedTables(new ArrayList<>(affectedTables))
            .affectedSystems(new ArrayList<>(affectedSystems))
            .breakingChanges(breakingChanges)
            .severity(calculateSeverity(downstream.getNodes().size(), breakingChanges.size()))
            .recommendedActions(generateRecommendations(breakingChanges))
            .build();
    }

    @Transactional
    public LineageRelationshipDTO createLineage(LineageRelationshipDTO dto, String username) {
        FieldMetadata source = fieldRepository.findById(dto.getSourceFieldId())
            .orElseThrow(() -> new ResourceNotFoundException("Source field not found"));
        FieldMetadata target = fieldRepository.findById(dto.getTargetFieldId())
            .orElseThrow(() -> new ResourceNotFoundException("Target field not found"));
        
        LineageRelationship lineage = LineageRelationship.builder()
            .sourceField(source)
            .targetField(target)
            .transformationLogic(dto.getTransformationLogic())
            .lineageType(LineageType.valueOf(dto.getLineageType()))
            .createdBy(username)
            .build();
        
        lineage = lineageRepository.save(lineage);
        
        // Record change
        changeHistoryRepository.save(ChangeHistory.builder()
            .entityType("LINEAGE")
            .entityId(lineage.getId())
            .action(ChangeAction.CREATED)
            .description("Created lineage relationship")
            .changedBy(username)
            .build());
        
        return mapToLineageDTO(lineage);
    }

    private LineageNodeDTO createNodeDTO(FieldMetadata field, int depth) {
        return LineageNodeDTO.builder()
            .fieldId(field.getId())
            .fieldName(field.getFieldName())
            .businessName(field.getBusinessName())
            .tableName(field.getTable().getTableName())
            .databaseName(field.getTable().getDatabase().getName())
            .dataType(field.getDataType())
            .sensitivityLevel(field.getSensitivityLevel().name())
            .depth(depth)
            .build();
    }

    private LineageEdgeDTO createEdgeDTO(LineageRelationship rel) {
        return LineageEdgeDTO.builder()
            .sourceFieldId(rel.getSourceField().getId())
            .targetFieldId(rel.getTargetField().getId())
            .lineageType(rel.getLineageType().name())
            .transformationLogic(rel.getTransformationLogic())
            .build();
    }

    private List<BreakingChangeDTO> identifyBreakingChanges(LineageGraphDTO downstream, 
                                                            ImpactAnalysisRequestDTO request) {
        List<BreakingChangeDTO> breakingChanges = new ArrayList<>();
        
        if ("TYPE_CHANGE".equals(request.getChangeType()) || 
            "DELETE".equals(request.getChangeType())) {
            for (LineageNodeDTO node : downstream.getNodes()) {
                breakingChanges.add(BreakingChangeDTO.builder()
                    .fieldId(node.getFieldId())
                    .fieldName(node.getFieldName())
                    .tableName(node.getTableName())
                    .reason("Direct dependency - change will break downstream")
                    .severity("HIGH")
                    .estimatedImpactHours(2)
                    .build());
            }
        }
        
        return breakingChanges;
    }

    private String calculateSeverity(int dependencyCount, int breakingCount) {
        if (breakingCount > 5 || dependencyCount > 20) return "CRITICAL";
        if (breakingCount > 2 || dependencyCount > 10) return "HIGH";
        if (breakingCount > 0 || dependencyCount > 5) return "MEDIUM";
        return "LOW";
    }

    private List<String> generateRecommendations(List<BreakingChangeDTO> breakingChanges) {
        List<String> recommendations = new ArrayList<>();
        if (!breakingChanges.isEmpty()) {
            recommendations.add("Create deprecation plan for 30 days before making changes");
            recommendations.add("Notify all data stewards and downstream system owners");
            recommendations.add("Update documentation and business glossary");
            recommendations.add("Run integration tests on all affected pipelines");
        }
        return recommendations;
    }

    private LineageRelationshipDTO mapToLineageDTO(LineageRelationship lineage) {
        return LineageRelationshipDTO.builder()
            .id(lineage.getId())
            .sourceFieldId(lineage.getSourceField().getId())
            .targetFieldId(lineage.getTargetField().getId())
            .transformationLogic(lineage.getTransformationLogic())
            .lineageType(lineage.getLineageType().name())
            .createdAt(lineage.getCreatedAt())
            .createdBy(lineage.getCreatedBy())
            .build();
    }
}
