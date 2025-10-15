package com.databricks.deltasharing.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * Configuração do Jackson ObjectMapper para formatação de JSON
 * 
 * Fornece dois ObjectMappers:
 * - objectMapper: Com pretty printing (2 espaços) para respostas JSON normais
 * - ndjsonObjectMapper: Sem pretty printing para respostas NDJSON (Newline-Delimited JSON)
 */
@Configuration
public class JacksonConfig {
    
    /**
     * ObjectMapper configurado com pretty printing para respostas JSON legíveis
     * Usado em endpoints REST normais (GET /shares, GET /schemas, etc.)
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
    
    /**
     * ObjectMapper SEM pretty printing para respostas NDJSON
     * Usado em endpoints que retornam NDJSON (POST /query, GET /metadata, GET /changes)
     * 
     * NDJSON (Newline-Delimited JSON) requer que cada linha seja um JSON completo e compacto
     * Exemplo correto:
     *   {"protocol":{"minReaderVersion":1}}
     *   {"metaData":{"id":"...","format":{"provider":"parquet"}}}
     * 
     * Exemplo INCORRETO (com indentação):
     *   {"protocol":{
     *     "minReaderVersion" : 1
     *   }}
     */
    @Bean
    @Qualifier("ndjsonObjectMapper")
    public ObjectMapper ndjsonObjectMapper(Jackson2ObjectMapperBuilder builder) {
        ObjectMapper mapper = builder.build();
        
        // DESABILITAR pretty printing para NDJSON
        mapper.disable(SerializationFeature.INDENT_OUTPUT);
        
        return mapper;
    }
}

