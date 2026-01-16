package com.vega.pushservice.domain.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.List;

@Data
public class PushRequest {
    
    @NotBlank(message = "Repository ID is required")
    private String repositoryId;
    
    @NotBlank(message = "Repository name is required")
    private String repositoryName;
    
    @NotBlank(message = "Commit hash is required")
    private String commitHash;
    
    @NotNull(message = "Files list is required")
    private List<FileInfo> files;
    
    @Data
    public static class FileInfo {
        private String path;
        private String content;
        private String hash;
        private Long size;
        private String type; // BLOB, TREE, COMMIT
    }
}




