package com.databricks.deltasharing.config;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;

/**
 * Filter para logar todas as requisições HTTP com detalhes completos
 * Facilita o debug mostrando URL completa, parâmetros, método HTTP e tempo de resposta
 * Em modo DEBUG, também loga os payloads de request e response
 */
@Slf4j
@Component
public class RequestLoggingFilter implements Filter {

    private static final int MAX_PAYLOAD_LENGTH = 10000; // Limitar tamanho do payload no log

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        
        String requestURI = httpRequest.getRequestURI();
        
        // Only apply logging filter to Delta Sharing API endpoints
        // Skip logging for web interface endpoints (admin UI, login, etc.)
        if (!requestURI.startsWith("/delta-sharing")) {
            chain.doFilter(request, response);
            return;
        }
        
        // Wrap request e response para poder ler o corpo múltiplas vezes
        ContentCachingRequestWrapper wrappedRequest = new ContentCachingRequestWrapper(httpRequest);
        ContentCachingResponseWrapper wrappedResponse = new ContentCachingResponseWrapper(httpResponse);
        
        long startTime = System.currentTimeMillis();
        
        // Construir URL completa
        String method = httpRequest.getMethod();
        String queryString = httpRequest.getQueryString();
        
        String fullURL = requestURI;
        if (queryString != null && !queryString.isEmpty()) {
            fullURL += "?" + queryString;
        }
        
        // Log da requisição inicial
        log.info("╔════════════════════════════════════════════════════════════════");
        log.info("║ 🔵 INCOMING REQUEST");
        log.info("║ Method: {} {}", method, fullURL);
        
        // Log dos parâmetros (opcional, mas útil para debug)
        Enumeration<String> parameterNames = httpRequest.getParameterNames();
        if (parameterNames.hasMoreElements()) {
            log.info("║ Parameters:");
            while (parameterNames.hasMoreElements()) {
                String paramName = parameterNames.nextElement();
                String paramValue = httpRequest.getParameter(paramName);
                log.info("║   - {}: {}", paramName, paramValue);
            }
        }
        
        // Log dos headers importantes
        String contentType = httpRequest.getContentType();
        if (contentType != null) {
            log.info("║ Content-Type: {}", contentType);
        }
        
        String authorization = httpRequest.getHeader("Authorization");
        if (authorization != null) {
            // Não mostrar o token completo por segurança
            log.info("║ Authorization: Bearer ***");
        }
        
        // Log completo de todos os headers em modo DEBUG
        if (log.isDebugEnabled()) {
            log.debug("║");
            log.debug("║ 📋 ALL REQUEST HEADERS:");
            Enumeration<String> headerNames = httpRequest.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                String headerName = headerNames.nextElement();
                String headerValue = httpRequest.getHeader(headerName);
                
                // Mascarar tokens sensíveis
                if (headerName.equalsIgnoreCase("Authorization") && headerValue != null) {
                    if (headerValue.startsWith("Bearer ")) {
                        headerValue = "Bearer " + maskToken(headerValue.substring(7));
                    }
                }
                
                log.debug("║   {}: {}", headerName, headerValue);
            }
        }
        
        log.info("╚════════════════════════════════════════════════════════════════");
        
        try {
            // Continuar com a cadeia de filtros
            chain.doFilter(wrappedRequest, wrappedResponse);
        } finally {
            // Log da resposta
            long duration = System.currentTimeMillis() - startTime;
            int status = wrappedResponse.getStatus();
            
            String statusEmoji = getStatusEmoji(status);
            
            log.info("╔════════════════════════════════════════════════════════════════");
            log.info("║ {} RESPONSE", statusEmoji);
            log.info("║ Method: {} {}", method, fullURL);
            log.info("║ Status: {}", status);
            log.info("║ Duration: {} ms", duration);
            
            // Log do payload e headers em modo DEBUG
            if (log.isDebugEnabled()) {
                // Log Response Headers
                log.debug("║");
                log.debug("║ 📋 ALL RESPONSE HEADERS:");
                for (String headerName : wrappedResponse.getHeaderNames()) {
                    String headerValue = wrappedResponse.getHeader(headerName);
                    log.debug("║   {}: {}", headerName, headerValue);
                }
                
                // Log Request Body (se houver)
                String requestPayload = getRequestPayload(wrappedRequest);
                if (requestPayload != null && !requestPayload.isEmpty()) {
                    log.debug("║");
                    log.debug("║ 📥 REQUEST BODY:");
                    logPayload(requestPayload, "║   ");
                }
                
                // Log Response Body (se houver)
                String responsePayload = getResponsePayload(wrappedResponse);
                if (responsePayload != null && !responsePayload.isEmpty()) {
                    log.debug("║");
                    log.debug("║ 📤 RESPONSE BODY:");
                    logPayload(responsePayload, "║   ");
                }
            }
            
            log.info("╚════════════════════════════════════════════════════════════════");
            
            // Importante: copiar o conteúdo de volta para a resposta original
            wrappedResponse.copyBodyToResponse();
        }
    }
    
    /**
     * Extrai o payload da requisição
     */
    private String getRequestPayload(ContentCachingRequestWrapper request) {
        byte[] content = request.getContentAsByteArray();
        if (content.length > 0) {
            try {
                String payload = new String(content, 0, Math.min(content.length, MAX_PAYLOAD_LENGTH), 
                                          request.getCharacterEncoding());
                return payload;
            } catch (UnsupportedEncodingException e) {
                return "[Unable to parse request payload]";
            }
        }
        return null;
    }
    
    /**
     * Extrai o payload da resposta
     */
    private String getResponsePayload(ContentCachingResponseWrapper response) {
        byte[] content = response.getContentAsByteArray();
        if (content.length > 0) {
            try {
                String payload = new String(content, 0, Math.min(content.length, MAX_PAYLOAD_LENGTH), 
                                          response.getCharacterEncoding());
                return payload;
            } catch (UnsupportedEncodingException e) {
                return "[Unable to parse response payload]";
            }
        }
        return null;
    }
    
    /**
     * Loga o payload com indentação e quebra de linhas
     */
    private void logPayload(String payload, String prefix) {
        // Se for muito longo, indicar que foi truncado
        if (payload.length() >= MAX_PAYLOAD_LENGTH) {
            payload = payload + "\n... [truncated - payload too large]";
        }
        
        // Logar linha por linha para melhor formatação
        String[] lines = payload.split("\n");
        for (String line : lines) {
            log.debug("{}{}", prefix, line);
        }
    }
    
    /**
     * Retorna emoji baseado no status HTTP
     */
    private String getStatusEmoji(int status) {
        if (status >= 200 && status < 300) {
            return "✅"; // Sucesso
        } else if (status >= 300 && status < 400) {
            return "🔄"; // Redirect
        } else if (status >= 400 && status < 500) {
            return "⚠️"; // Erro do cliente
        } else if (status >= 500) {
            return "❌"; // Erro do servidor
        }
        return "ℹ️"; // Informacional
    }
    
    /**
     * Mascarar token para segurança - mostra apenas primeiros e últimos caracteres
     */
    private String maskToken(String token) {
        if (token == null || token.length() <= 10) {
            return "***";
        }
        String prefix = token.substring(0, 4);
        String suffix = token.substring(token.length() - 4);
        return prefix + "..." + suffix;
    }
}

