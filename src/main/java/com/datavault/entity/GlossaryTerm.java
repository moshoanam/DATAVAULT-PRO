package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

@Entity
@Table(name = "glossary_terms", indexes = {
    @Index(name = "idx_term", columnList = "term"),
    @Index(name = "idx_category", columnList = "category")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GlossaryTerm {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String term;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String definition;

    private String category;

    @ElementCollection
    @CollectionTable(name = "glossary_related_terms", joinColumns = @JoinColumn(name = "term_id"))
    @Column(name = "related_term")
    private List<String> relatedTerms = new ArrayList<>();

    @ElementCollection
    @CollectionTable(name = "glossary_synonyms", joinColumns = @JoinColumn(name = "term_id"))
    @Column(name = "synonym")
    private List<String> synonyms = new ArrayList<>();

    @OneToMany(mappedBy = "glossaryTerm")
    private List<FieldMetadata> usedInFields = new ArrayList<>();

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    private String createdBy;
    private String lastModifiedBy;

    @Version
    private Long version;
}
