package com.databricks.deltasharing.repository;

import com.databricks.deltasharing.model.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for User entity
 * Used for web interface authentication
 */
@Repository
public interface UserRepository extends JpaRepository<User, Long> {
    
    /**
     * Find user by username
     * @param username the username
     * @return Optional containing the user if found
     */
    Optional<User> findByUsername(String username);
    
    /**
     * Check if username already exists
     * @param username the username
     * @return true if exists, false otherwise
     */
    boolean existsByUsername(String username);
    
    /**
     * Check if email already exists
     * @param email the email
     * @return true if exists, false otherwise
     */
    boolean existsByEmail(String email);
    
    /**
     * Find user by API token
     * @param apiToken the API token
     * @return Optional containing the user if found
     */
    Optional<User> findByApiToken(String apiToken);
}

