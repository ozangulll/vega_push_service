package com.vega.pushservice.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PushResponse {
    
    private Long pushId;
    private String repositoryId;
    private String repositoryName;
    private String hdfsPath;
    private String status;
    private Integer fileCount;
    private Long totalSize;
    private LocalDateTime createdAt;
    private String message;
}




