package com.databricks.deltasharing.config;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Enumeration;

/**
 * Filter para logar todas as requisições HTTP com detalhes completos
 * Facilita o debug mostrando URL completa, parâmetros, método HTTP e tempo de resposta
 */
@Slf4j
@Component
public class RequestLoggingFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        
        long startTime = System.currentTimeMillis();
        
        // Construir URL completa
        String method = httpRequest.getMethod();
        String requestURI = httpRequest.getRequestURI();
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
        
        // Log dos headers importantes (opcional)
        String contentType = httpRequest.getContentType();
        if (contentType != null) {
            log.info("║ Content-Type: {}", contentType);
        }
        
        String authorization = httpRequest.getHeader("Authorization");
        if (authorization != null) {
            // Não mostrar o token completo por segurança
            log.info("║ Authorization: Bearer ***");
        }
        
        log.info("╚════════════════════════════════════════════════════════════════");
        
        try {
            // Continuar com a cadeia de filtros
            chain.doFilter(request, response);
        } finally {
            // Log da resposta
            long duration = System.currentTimeMillis() - startTime;
            int status = httpResponse.getStatus();
            
            String statusEmoji = getStatusEmoji(status);
            
            log.info("╔════════════════════════════════════════════════════════════════");
            log.info("║ {} RESPONSE", statusEmoji);
            log.info("║ Method: {} {}", method, fullURL);
            log.info("║ Status: {}", status);
            log.info("║ Duration: {} ms", duration);
            log.info("╚════════════════════════════════════════════════════════════════");
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
}

