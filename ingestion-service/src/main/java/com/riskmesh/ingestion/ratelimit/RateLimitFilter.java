package com.riskmesh.ingestion.ratelimit;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.filter.OncePerRequestFilter;


public class RateLimitFilter extends OncePerRequestFilter {

    private static final String TRANSACTIONS_PATH = "/api/v1/transactions";
    private static final String MERCHANT_HEADER = "X-Merchant-Id";

    private final RedisRateLimiter rateLimiter;
    private final IpBlocklistService blocklistService;
    private final ObjectMapper objectMapper;

    public RateLimitFilter(RedisRateLimiter rateLimiter, IpBlocklistService blocklistService, ObjectMapper objectMapper) {
        this.rateLimiter = rateLimiter;
        this.blocklistService = blocklistService;
        this.objectMapper = objectMapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !("POST".equalsIgnoreCase(request.getMethod()) && TRANSACTIONS_PATH.equals(request.getRequestURI()));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String ip = request.getRemoteAddr();

        if (blocklistService.isBlocked(ip)) {
            reject(response, HttpStatus.FORBIDDEN, "IP_BLOCKED",
                    "This IP address is temporarily blocked due to repeated rate-limit violations");
            return;
        }

        String merchantIdHeader = request.getHeader(MERCHANT_HEADER);
        if (merchantIdHeader != null) {
            try {
                UUID merchantId = UUID.fromString(merchantIdHeader);
                RateLimitResult merchantResult = rateLimiter.checkMerchant(merchantId);
                if (!merchantResult.allowed()) {
                    blocklistService.recordViolation(ip);
                    rejectRateLimited(response, merchantResult.retryAfterSeconds());
                    return;
                }
            } catch (IllegalArgumentException ignored) {

            }
        }

        RateLimitResult globalResult = rateLimiter.checkGlobal();
        if (!globalResult.allowed()) {
            blocklistService.recordViolation(ip);
            rejectRateLimited(response, globalResult.retryAfterSeconds());
            return;
        }

        chain.doFilter(request, response);
    }

    private void rejectRateLimited(HttpServletResponse response, long retryAfterSeconds) throws IOException {
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader("Retry-After", String.valueOf(Math.max(retryAfterSeconds, 0)));
        response.setContentType("application/json");
        ProblemDetail problemDetail = ProblemDetail.forStatus(HttpStatus.TOO_MANY_REQUESTS);
        problemDetail.setTitle("RATE_LIMIT_EXCEEDED");
        problemDetail.setProperty("retryAfter", retryAfterSeconds);
        response.getWriter().write(objectMapper.writeValueAsString(problemDetail));
    }

    private void reject(HttpServletResponse response, HttpStatus status, String title, String detail) throws IOException {
        response.setStatus(status.value());
        response.setContentType("application/json");
        ProblemDetail problemDetail = ProblemDetail.forStatus(status);
        problemDetail.setTitle(title);
        problemDetail.setDetail(detail);
        response.getWriter().write(objectMapper.writeValueAsString(problemDetail));
    }
}
