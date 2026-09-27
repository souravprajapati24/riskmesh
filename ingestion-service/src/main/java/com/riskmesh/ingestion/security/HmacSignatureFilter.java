package com.riskmesh.ingestion.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.filter.OncePerRequestFilter;


public class HmacSignatureFilter extends OncePerRequestFilter {

    private static final String SIGNATURE_HEADER = "X-Gateway-Signature";
    private static final String MERCHANT_HEADER = "X-Merchant-Id";
    private static final String TRANSACTIONS_PATH = "/api/v1/transactions";

    private final MerchantSecretResolver secretResolver;
    private final ObjectMapper objectMapper;

    public HmacSignatureFilter(MerchantSecretResolver secretResolver, ObjectMapper objectMapper) {
        this.secretResolver = secretResolver;
        this.objectMapper = objectMapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !("POST".equalsIgnoreCase(request.getMethod()) && TRANSACTIONS_PATH.equals(request.getRequestURI()));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {

        CachedBodyHttpServletRequest wrapped = new CachedBodyHttpServletRequest(request);
        byte[] body = wrapped.getCachedBody();

        String provided = request.getHeader(SIGNATURE_HEADER);
        if (provided == null || !provided.startsWith("sha256=")) {
            reject(response, "Missing or malformed X-Gateway-Signature header");
            return;
        }

        String merchantId = request.getHeader(MERCHANT_HEADER);
        byte[] secret = secretResolver.resolve(merchantId);
        String computed = "sha256=" + hexHmacSha256(secret, body);

        if (!MessageDigest.isEqual(computed.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8))) {
            reject(response, "Signature does not match the request body");
            return;
        }

        chain.doFilter(wrapped, response);
    }

    private String hexHmacSha256(byte[] key, byte[] message) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(message));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Unable to compute HMAC-SHA256 signature", e);
        }
    }

    private void reject(HttpServletResponse response, String detail) throws IOException {
        response.setStatus(HttpStatus.UNAUTHORIZED.value());
        response.setContentType("application/json");
        ProblemDetail problemDetail = ProblemDetail.forStatus(HttpStatus.UNAUTHORIZED);
        problemDetail.setTitle("INVALID_SIGNATURE");
        problemDetail.setDetail(detail);
        response.getWriter().write(objectMapper.writeValueAsString(problemDetail));
    }
}
