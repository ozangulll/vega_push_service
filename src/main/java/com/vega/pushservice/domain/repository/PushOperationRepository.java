package com.vega.pushservice.domain.repository;

import com.vega.pushservice.domain.model.PushOperation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PushOperationRepository extends JpaRepository<PushOperation, Long> {
    
    List<PushOperation> findByUserIdOrderByCreatedAtDesc(Long userId);
    
    Optional<PushOperation> findByIdAndUserId(Long id, Long userId);
    
    List<PushOperation> findByRepositoryIdAndUserId(String repositoryId, Long userId);
}




