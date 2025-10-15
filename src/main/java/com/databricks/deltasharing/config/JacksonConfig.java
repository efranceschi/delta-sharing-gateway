package com.databricks.deltasharing.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * Configuração do Jackson ObjectMapper para formatação de JSON
 * Configura pretty printing com indentação de 2 espaços
 */
@Configuration
public class JacksonConfig {
    
    /**
     * ObjectMapper configurado com pretty printing para respostas legíveis
     * Indentação: 2 espaços
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper(Jackson2ObjectMapperBuilder builder) {
        ObjectMapper mapper = builder.build();
        
        // Habilitar pretty printing
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        
        // Configurar indentação de 2 espaços (default é 2, mas explicitando)
        mapper.writerWithDefaultPrettyPrinter();
        
        return mapper;
    }
}

