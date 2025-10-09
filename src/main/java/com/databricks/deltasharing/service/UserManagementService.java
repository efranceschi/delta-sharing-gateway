package com.databricks.deltasharing.service;

import com.databricks.deltasharing.dto.UserDTO;
import com.databricks.deltasharing.exception.ResourceNotFoundException;
import com.databricks.deltasharing.model.User;
import com.databricks.deltasharing.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserManagementService {
    
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    
    @Value("${delta.sharing.token.expiration-days:365}")
    private long tokenExpirationDays;
    
    @Transactional(readOnly = true)
    public List<UserDTO> getAllUsers() {
        return userRepository.findAll().stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }
    
    @Transactional(readOnly = true)
    public UserDTO getUserById(Long id) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("User not found with id: " + id));
        return convertToDTO(user);
    }
    
    @Transactional
    public UserDTO createUser(UserDTO dto) {
        log.info("Creating new user: {}", dto.getUsername());
        
        if (userRepository.existsByUsername(dto.getUsername())) {
            throw new IllegalArgumentException("Username already exists: " + dto.getUsername());
        }
        
        User user = User.builder()
                .username(dto.getUsername())
                .password(passwordEncoder.encode(dto.getPassword()))
                .fullName(dto.getFullName())
                .email(dto.getEmail())
                .role(dto.getRole() != null ? dto.getRole() : "USER")
                .enabled(dto.getEnabled() != null ? dto.getEnabled() : true)
                .active(dto.getActive() != null ? dto.getActive() : true)
                .accountNonExpired(true)
                .accountNonLocked(true)
                .credentialsNonExpired(true)
                .build();
        
        user = userRepository.save(user);
        log.info("User created successfully: {}", user.getUsername());
        
        return convertToDTO(user);
    }
    
    @Transactional
    public UserDTO updateUser(Long id, UserDTO dto) {
        log.info("Updating user with id: {}", id);
        
        User user = userRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("User not found with id: " + id));
        
        // Check if username is being changed and if it already exists
        if (!user.getUsername().equals(dto.getUsername()) && 
            userRepository.existsByUsername(dto.getUsername())) {
            throw new IllegalArgumentException("Username already exists: " + dto.getUsername());
        }
        
        user.setUsername(dto.getUsername());
        user.setFullName(dto.getFullName());
        user.setEmail(dto.getEmail());
        user.setRole(dto.getRole());
        user.setEnabled(dto.getEnabled());
        user.setActive(dto.getActive());
        
        // Only update password if provided
        if (dto.getPassword() != null && !dto.getPassword().isEmpty()) {
            user.setPassword(passwordEncoder.encode(dto.getPassword()));
        }
        
        user = userRepository.save(user);
        log.info("User updated successfully: {}", user.getUsername());
        
        return convertToDTO(user);
    }
    
    @Transactional
    public void deleteUser(Long id) {
        log.info("Deleting user with id: {}", id);
        
        if (!userRepository.existsById(id)) {
            throw new ResourceNotFoundException("User not found with id: " + id);
        }
        
        userRepository.deleteById(id);
        log.info("User deleted successfully with id: {}", id);
    }
    
    @Transactional
    public String generateApiToken(Long userId) {
        log.info("Generating API token for user id: {}", userId);
        
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new ResourceNotFoundException("User not found with id: " + userId));
        
        // Check if user has Enable API enabled
        if (!user.getActive()) {
            throw new IllegalStateException("Cannot generate token: Enable API is disabled for this user");
        }
        
        // Generate a secure random token with exactly 65 characters
        // Format: dss_ (4 chars) + 61 random alphanumeric chars = 65 total
        String token = "dss_" + generateSecureRandomString(61);
        
        // Set token and expiration
        user.setApiToken(token);
        user.setTokenExpiresAt(LocalDateTime.now().plusDays(tokenExpirationDays));
        
        userRepository.save(user);
        log.info("API token generated successfully for user: {} (expires in {} days)", 
                 user.getUsername(), tokenExpirationDays);
        
        return token;
    }
    
    /**
     * Generate a secure random alphanumeric string
     */
    private String generateSecureRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        SecureRandom random = new SecureRandom();
        StringBuilder sb = new StringBuilder(length);
        
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        
        return sb.toString();
    }
    
    @Transactional
    public void revokeApiToken(Long userId) {
        log.info("Revoking API token for user id: {}", userId);
        
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new ResourceNotFoundException("User not found with id: " + userId));
        
        user.setApiToken(null);
        user.setTokenExpiresAt(null);
        
        userRepository.save(user);
        log.info("API token revoked successfully for user: {}", user.getUsername());
    }
    
    @Transactional(readOnly = true)
    public User findByApiToken(String token) {
        return userRepository.findByApiToken(token).orElse(null);
    }
    
    @Transactional(readOnly = true)
    public boolean isTokenValid(String token) {
        User user = userRepository.findByApiToken(token).orElse(null);
        if (user == null || !user.getActive()) {
            return false;
        }
        
        // Check if token is expired
        if (user.getTokenExpiresAt() != null && 
            LocalDateTime.now().isAfter(user.getTokenExpiresAt())) {
            return false;
        }
        
        return true;
    }
    
    private UserDTO convertToDTO(User user) {
        return UserDTO.builder()
                .id(user.getId())
                .username(user.getUsername())
                .fullName(user.getFullName())
                .email(user.getEmail())
                .role(user.getRole())
                .enabled(user.getEnabled())
                .active(user.getActive())
                .apiToken(user.getApiToken())
                .tokenExpiresAt(user.getTokenExpiresAt())
                .createdAt(user.getCreatedAt())
                .updatedAt(user.getUpdatedAt())
                .build();
    }
}

