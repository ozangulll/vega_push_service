package com.vega.pushservice.domain.service;

import com.vega.pushservice.domain.dto.PushRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.io.ByteArrayOutputStream;

@Service
@Slf4j
public class HdfsService {
    
    @Value("${hadoop.hdfs.uri}")
    private String hdfsUri;
    
    @Value("${hadoop.hdfs.base-path}")
    private String basePath;
    
    @Value("${hadoop.hdfs.replication}")
    private short replication;
    
    @Value("${hadoop.hdfs.block-size}")
    private long blockSize;
    
    private FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("dfs.replication", String.valueOf(replication));
        return FileSystem.get(conf);
    }
    
    public String uploadRepository(Long userId, String repositoryId, PushRequest pushRequest) throws IOException {
        String hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
        
        try (FileSystem fs = getFileSystem()) {
            // Create directory structure
            Path repoPath = new Path(hdfsPath);
            if (!fs.exists(repoPath)) {
                fs.mkdirs(repoPath);
            }
            
            // Upload each file
            for (PushRequest.FileInfo file : pushRequest.getFiles()) {
                String filePath = String.format("%s/%s", hdfsPath, file.getPath());
                Path hdfsFilePath = new Path(filePath);
                
                // Create parent directories if they don't exist
                Path parentDir = hdfsFilePath.getParent();
                if (parentDir != null && !fs.exists(parentDir)) {
                    fs.mkdirs(parentDir);
                }
                
                // Compress and upload file
                byte[] compressedData = compressData(file.getContent().getBytes());
                try (InputStream inputStream = new ByteArrayInputStream(compressedData);
                     FSDataOutputStream outputStream = fs.create(hdfsFilePath, true, 4096, replication, blockSize)) {
                    
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                }
                
                log.info("Uploaded file: {} to HDFS path: {}", file.getPath(), filePath);
            }
            
            // Create metadata file
            createMetadataFile(fs, hdfsPath, pushRequest);
            
            return hdfsPath;
        }
    }
    
    public String downloadRepository(Long userId, String repositoryId) throws IOException {
        String hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
        
        try (FileSystem fs = getFileSystem()) {
            Path repoPath = new Path(hdfsPath);
            if (!fs.exists(repoPath)) {
                throw new IOException("Repository not found: " + repositoryId);
            }
            
            // List all files in the repository
            RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(repoPath, true);
            StringBuilder repositoryContent = new StringBuilder();
            
            while (fileIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileIterator.next();
                if (fileStatus.isFile()) {
                    String relativePath = fileStatus.getPath().toString().substring(hdfsPath.length() + 1);
                    repositoryContent.append(relativePath).append("\n");
                }
            }
            
            return repositoryContent.toString();
        }
    }
    
    public boolean repositoryExists(Long userId, String repositoryId) throws IOException {
        String hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
        
        try (FileSystem fs = getFileSystem()) {
            return fs.exists(new Path(hdfsPath));
        }
    }
    
    public void deleteRepository(Long userId, String repositoryId) throws IOException {
        String hdfsPath = String.format("%s/%d/%s", basePath, userId, repositoryId);
        
        try (FileSystem fs = getFileSystem()) {
            Path repoPath = new Path(hdfsPath);
            if (fs.exists(repoPath)) {
                fs.delete(repoPath, true);
                log.info("Deleted repository: {} from HDFS path: {}", repositoryId, hdfsPath);
            }
        }
    }
    
    private byte[] compressData(byte[] data) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            
            gzipOut.write(data);
            gzipOut.finish();
            return baos.toByteArray();
        }
    }
    
    private byte[] decompressData(byte[] compressedData) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
             GZIPInputStream gzipIn = new GZIPInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = gzipIn.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            return baos.toByteArray();
        }
    }
    
    private void createMetadataFile(FileSystem fs, String hdfsPath, PushRequest pushRequest) throws IOException {
        String metadataContent = String.format(
            "repository_id=%s\nrepository_name=%s\ncommit_hash=%s\nfile_count=%d\n",
            pushRequest.getRepositoryId(),
            pushRequest.getRepositoryName(),
            pushRequest.getCommitHash(),
            pushRequest.getFiles().size()
        );
        
        Path metadataPath = new Path(hdfsPath + "/.vega-metadata");
        try (InputStream inputStream = new ByteArrayInputStream(metadataContent.getBytes());
             FSDataOutputStream outputStream = fs.create(metadataPath, true, 4096, replication, blockSize)) {
            
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }
    }
}




