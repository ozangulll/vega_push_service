package com.vega.pushservice.domain.repository;

import com.vega.pushservice.domain.model.RepositoryMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface RepositoryMetadataRepository extends JpaRepository<RepositoryMetadata, Long> {
    
    List<RepositoryMetadata> findByUserIdOrderByUpdatedAtDesc(Long userId);
    
    Optional<RepositoryMetadata> findByRepositoryIdAndUserId(String repositoryId, Long userId);
    
    Optional<RepositoryMetadata> findByRepositoryId(String repositoryId);
    
    boolean existsByRepositoryIdAndUserId(String repositoryId, Long userId);
}




