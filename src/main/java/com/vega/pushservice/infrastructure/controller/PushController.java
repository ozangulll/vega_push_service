package com.vega.pushservice.infrastructure.controller;

import com.vega.pushservice.domain.dto.PushRequest;
import com.vega.pushservice.domain.dto.PushResponse;
import com.vega.pushservice.domain.model.RepositoryMetadata;
import com.vega.pushservice.domain.service.PushService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/push")
@RequiredArgsConstructor
public class PushController {
    
    private final PushService pushService;
    
    @PostMapping("/repository")
    public ResponseEntity<PushResponse> pushRepository(
            @RequestHeader("Authorization") String token,
            @Valid @RequestBody PushRequest request) {
        try {
            PushResponse response = pushService.pushRepository(token, request);
            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().build();
        }
    }
    
    @GetMapping("/history")
    public ResponseEntity<List<PushResponse>> getPushHistory(
            @RequestHeader("Authorization") String token) {
        try {
            List<PushResponse> history = pushService.getPushHistory(token);
            return ResponseEntity.ok(history);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().build();
        }
    }
    
    @GetMapping("/status/{pushId}")
    public ResponseEntity<PushResponse> getPushStatus(
            @RequestHeader("Authorization") String token,
            @PathVariable Long pushId) {
        try {
            PushResponse status = pushService.getPushStatus(token, pushId);
            return ResponseEntity.ok(status);
        } catch (RuntimeException e) {
            return ResponseEntity.notFound().build();
        }
    }
}

@RestController
@RequestMapping("/api/repositories")
@RequiredArgsConstructor
class RepositoryController {
    
    private final PushService pushService;
    
    @GetMapping
    public ResponseEntity<List<RepositoryMetadata>> getUserRepositories(
            @RequestHeader("Authorization") String token) {
        try {
            List<RepositoryMetadata> repositories = pushService.getUserRepositories(token);
            return ResponseEntity.ok(repositories);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().build();
        }
    }
    
    @DeleteMapping("/{repositoryId}")
    public ResponseEntity<Void> deleteRepository(
            @RequestHeader("Authorization") String token,
            @PathVariable String repositoryId) {
        try {
            pushService.deleteRepository(token, repositoryId);
            return ResponseEntity.ok().build();
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().build();
        }
    }
}




