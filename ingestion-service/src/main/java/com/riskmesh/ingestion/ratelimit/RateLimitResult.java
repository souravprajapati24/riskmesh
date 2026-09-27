package com.riskmesh.ingestion.ratelimit;

public record RateLimitResult(boolean allowed, long retryAfterSeconds) {
    /* public static RateLimitResult allowed() {
        return new RateLimitResult(true, -1);
    }*/
}
