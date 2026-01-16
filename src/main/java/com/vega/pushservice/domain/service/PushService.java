package com.vega.pushservice.domain.service;

import com.vega.pushservice.domain.dto.PushRequest;
import com.vega.pushservice.domain.dto.PushResponse;
import com.vega.pushservice.domain.model.PushOperation;
import com.vega.pushservice.domain.model.RepositoryMetadata;
import com.vega.pushservice.domain.repository.PushOperationRepository;
import com.vega.pushservice.domain.repository.RepositoryMetadataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PushService {
    
    private final HdfsService hdfsService;
    private final UserValidationService userValidationService;
    private final PushOperationRepository pushOperationRepository;
    private final RepositoryMetadataRepository repositoryMetadataRepository;
    
    @Transactional
    public PushResponse pushRepository(String token, PushRequest request) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        // Create push operation record
        PushOperation pushOperation = PushOperation.builder()
                .userId(userId)
                .repositoryId(request.getRepositoryId())
                .repositoryName(request.getRepositoryName())
                .hdfsPath("") // Will be updated after upload
                .status(PushOperation.Status.PENDING)
                .fileCount(request.getFiles().size())
                .totalSize(calculateTotalSize(request))
                .build();
        
        pushOperation = pushOperationRepository.save(pushOperation);
        
        try {
            // Update status to in progress
            pushOperation.setStatus(PushOperation.Status.IN_PROGRESS);
            pushOperationRepository.save(pushOperation);
            
            // Upload to HDFS
            String hdfsPath = hdfsService.uploadRepository(userId, request.getRepositoryId(), request);
            pushOperation.setHdfsPath(hdfsPath);
            
            // Update or create repository metadata
            updateRepositoryMetadata(userId, request, hdfsPath);
            
            // Mark as completed
            pushOperation.setStatus(PushOperation.Status.COMPLETED);
            pushOperation.setCompletedAt(LocalDateTime.now());
            pushOperationRepository.save(pushOperation);
            
            log.info("Successfully pushed repository: {} for user: {}", request.getRepositoryId(), userId);
            
            return PushResponse.builder()
                    .pushId(pushOperation.getId())
                    .repositoryId(request.getRepositoryId())
                    .repositoryName(request.getRepositoryName())
                    .hdfsPath(hdfsPath)
                    .status(pushOperation.getStatus().name())
                    .fileCount(pushOperation.getFileCount())
                    .totalSize(pushOperation.getTotalSize())
                    .createdAt(pushOperation.getCreatedAt())
                    .message("Repository pushed successfully")
                    .build();
                    
        } catch (Exception e) {
            // Mark as failed
            pushOperation.setStatus(PushOperation.Status.FAILED);
            pushOperationRepository.save(pushOperation);
            
            log.error("Failed to push repository: {} for user: {}", request.getRepositoryId(), userId, e);
            throw new RuntimeException("Failed to push repository: " + e.getMessage());
        }
    }
    
    public List<PushResponse> getPushHistory(String token) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        List<PushOperation> operations = pushOperationRepository.findByUserIdOrderByCreatedAtDesc(userId);
        
        return operations.stream()
                .map(this::mapToPushResponse)
                .toList();
    }
    
    public PushResponse getPushStatus(String token, Long pushId) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        PushOperation operation = pushOperationRepository.findByIdAndUserId(pushId, userId)
                .orElseThrow(() -> new RuntimeException("Push operation not found"));
        
        return mapToPushResponse(operation);
    }
    
    public List<RepositoryMetadata> getUserRepositories(String token) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        return repositoryMetadataRepository.findByUserIdOrderByUpdatedAtDesc(userId);
    }
    
    @Transactional
    public void deleteRepository(String token, String repositoryId) {
        // Validate user token
        if (!userValidationService.validateToken(token)) {
            throw new RuntimeException("Invalid or expired token");
        }
        
        Long userId = userValidationService.getUserIdFromToken(token);
        if (userId == null) {
            throw new RuntimeException("Unable to determine user ID");
        }
        
        // Check if repository belongs to user
        RepositoryMetadata metadata = repositoryMetadataRepository.findByRepositoryIdAndUserId(repositoryId, userId)
                .orElseThrow(() -> new RuntimeException("Repository not found or access denied"));
        
        try {
            // Delete from HDFS
            hdfsService.deleteRepository(userId, repositoryId);
            
            // Delete metadata
            repositoryMetadataRepository.delete(metadata);
            
            log.info("Successfully deleted repository: {} for user: {}", repositoryId, userId);
        } catch (Exception e) {
            log.error("Failed to delete repository: {} for user: {}", repositoryId, userId, e);
            throw new RuntimeException("Failed to delete repository: " + e.getMessage());
        }
    }
    
    private void updateRepositoryMetadata(Long userId, PushRequest request, String hdfsPath) {
        RepositoryMetadata existingMetadata = repositoryMetadataRepository
                .findByRepositoryIdAndUserId(request.getRepositoryId(), userId)
                .orElse(null);
        
        if (existingMetadata != null) {
            // Update existing metadata
            existingMetadata.setRepositoryName(request.getRepositoryName());
            existingMetadata.setHdfsPath(hdfsPath);
            existingMetadata.setLastCommitHash(request.getCommitHash());
            existingMetadata.setFileCount(request.getFiles().size());
            existingMetadata.setTotalSize(calculateTotalSize(request));
            repositoryMetadataRepository.save(existingMetadata);
        } else {
            // Create new metadata
            RepositoryMetadata metadata = RepositoryMetadata.builder()
                    .repositoryId(request.getRepositoryId())
                    .userId(userId)
                    .repositoryName(request.getRepositoryName())
                    .hdfsPath(hdfsPath)
                    .lastCommitHash(request.getCommitHash())
                    .fileCount(request.getFiles().size())
                    .totalSize(calculateTotalSize(request))
                    .build();
            repositoryMetadataRepository.save(metadata);
        }
    }
    
    private long calculateTotalSize(PushRequest request) {
        return request.getFiles().stream()
                .mapToLong(PushRequest.FileInfo::getSize)
                .sum();
    }
    
    private PushResponse mapToPushResponse(PushOperation operation) {
        return PushResponse.builder()
                .pushId(operation.getId())
                .repositoryId(operation.getRepositoryId())
                .repositoryName(operation.getRepositoryName())
                .hdfsPath(operation.getHdfsPath())
                .status(operation.getStatus().name())
                .fileCount(operation.getFileCount())
                .totalSize(operation.getTotalSize())
                .createdAt(operation.getCreatedAt())
                .message(operation.getStatus() == PushOperation.Status.COMPLETED ? 
                    "Repository pushed successfully" : 
                    "Push operation " + operation.getStatus().name().toLowerCase())
                .build();
    }
}




