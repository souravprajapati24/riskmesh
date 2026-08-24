package com.riskmesh.common.web;

import java.net.URI;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ValidationException.class)
    public ProblemDetail handleValidation(ValidationException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        problemDetail.setType(URI.create("https://riskmesh.dev/errors/validation-error"));
        problemDetail.setTitle("VALIDATION_ERROR");
        problemDetail.setProperty("details", ex.getViolations());
        problemDetail.setProperty("traceId", MDC.get("traceId"));
        return problemDetail;
    }

    @ExceptionHandler(InvalidSignatureException.class)
    public ProblemDetail handleInvalidSignature(InvalidSignatureException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatus(HttpStatus.UNAUTHORIZED);
        problemDetail.setType(URI.create("https://riskmesh.dev/errors/invalid-signature"));
        problemDetail.setTitle("INVALID_SIGNATURE");
        return problemDetail;
    }

    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<ProblemDetail> handleRateLimit(RateLimitExceededException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatus(HttpStatus.TOO_MANY_REQUESTS);
        problemDetail.setType(URI.create("https://riskmesh.dev/errors/rate-limit-exceeded"));
        problemDetail.setTitle("RATE_LIMIT_EXCEEDED");
        problemDetail.setProperty("retryAfter", ex.getRetryAfterSeconds());
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", String.valueOf(ex.getRetryAfterSeconds()))
                .body(problemDetail);
    }
}
