package com.databricks.deltasharing.config;

import com.databricks.deltasharing.security.BearerTokenAuthenticationFilter;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Security configuration for Delta Sharing
 * Implements Bearer Token authentication
 */
@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {
    
    private final BearerTokenAuthenticationFilter bearerTokenAuthenticationFilter;
    
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
            .csrf(csrf -> csrf.disable())
            .sessionManagement(session -> 
                session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                // Allow web UI and public endpoints
                .requestMatchers(
                    "/",
                    "/shares",
                    "/shares/**",
                    "/api/v1/**",
                    "/swagger-ui/**",
                    "/swagger-ui.html",
                    "/v3/api-docs/**",
                    "/api-docs/**",
                    "/h2-console/**",
                    "/css/**",
                    "/js/**",
                    "/images/**"
                ).permitAll()
                // Require authentication for Delta Sharing endpoints
                .requestMatchers("/delta-sharing/**").authenticated()
                .anyRequest().permitAll()
            )
            .addFilterBefore(bearerTokenAuthenticationFilter, 
                           UsernamePasswordAuthenticationFilter.class)
            .headers(headers -> headers
                .frameOptions(frame -> frame.sameOrigin()) // For H2 console
            );
        
        return http.build();
    }
}
