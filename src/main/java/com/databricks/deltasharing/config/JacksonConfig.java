package com.databricks.deltasharing.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * Configuração do Jackson ObjectMapper para formatação de JSON
 * 
 * Fornece dois ObjectMappers:
 * - objectMapper: Configurável via application.yml (pretty print ou compacto)
 * - ndjsonObjectMapper: Sem pretty printing para respostas NDJSON (Newline-Delimited JSON)
 * 
 * O formato de saída JSON pode ser controlado pela propriedade:
 * delta.sharing.json.pretty-print (default: false)
 */
@Configuration
public class JacksonConfig {
    
    @Value("${delta.sharing.json.pretty-print:false}")
    private boolean prettyPrint;
    
    /**
     * ObjectMapper configurado para respostas JSON
     * Usado em endpoints REST normais (GET /shares, GET /schemas, etc.)
     * 
     * A formatação (pretty print) é configurável via application.yml:
     * - delta.sharing.json.pretty-print: false (default) - JSON compacto em uma linha
     * - delta.sharing.json.pretty-print: true - JSON formatado com indentação
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper(Jackson2ObjectMapperBuilder builder) {
        ObjectMapper mapper = builder.build();
        
        // Aplicar configuração de pretty printing
        if (prettyPrint) {
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
        } else {
            mapper.disable(SerializationFeature.INDENT_OUTPUT);
        }
        
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

