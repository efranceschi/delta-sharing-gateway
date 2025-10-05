package com.databricks.deltasharing.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * OpenAPI configuration for Delta Sharing Protocol
 */
@Configuration
public class DeltaSharingOpenApiConfig {
    
    @Bean
    public OpenAPI deltaSharingOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Delta Sharing Protocol API")
                        .version("1.0.0")
                        .description("REST API implementing the Delta Sharing Protocol for secure data sharing.\n\n" +
                                   "Based on the official Delta Sharing Protocol specification: " +
                                   "https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md\n\n" +
                                   "**Authentication:** All Delta Sharing endpoints require Bearer Token authentication.")
                        .contact(new Contact()
                                .name("Databricks Team")
                                .email("support@databricks.com")
                                .url("https://delta.io/sharing"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0.html")))
                .servers(List.of(
                        new Server()
                                .url("http://localhost:8080")
                                .description("Development Server")
                ))
                .components(new Components()
                        .addSecuritySchemes("bearerAuth", 
                                new SecurityScheme()
                                        .type(SecurityScheme.Type.HTTP)
                                        .scheme("bearer")
                                        .bearerFormat("token")
                                        .description("Bearer Token for Delta Sharing authentication")));
    }
}
