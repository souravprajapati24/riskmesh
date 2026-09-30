package com.riskmesh.ingestion.service;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);
    private static final Duration REDIS_TTL = Duration.ofHours(72);
    private static final Duration POSTGRES_TTL = Duration.ofHours(72);
    private static final String CACHE_KEY_PREFIX = "dedup:";

    private final StringRedisTemplate redisTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;

    public IdempotencyService(StringRedisTemplate redisTemplate, JdbcTemplate jdbcTemplate, Clock clock) {
        this.redisTemplate = redisTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    public IdempotencyResult checkAndReserve(String idempotencyKey, UUID candidateTransactionId) {
        try {
            String cached = redisTemplate.opsForValue().get(CACHE_KEY_PREFIX + idempotencyKey);
            if (cached != null) {
                return IdempotencyResult.duplicate(UUID.fromString(cached));
            }
        } catch (RedisConnectionFailureException e) {
            log.warn("Redis unavailable for idempotency cache read; falling through to PostgreSQL", e);
        }

        Instant now = Instant.now(clock);
        Map<String, Object> row = jdbcTemplate.queryForMap("""
                INSERT INTO deduplication_records (idempotency_key, transaction_id, first_seen_at, ttl_expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
                RETURNING transaction_id, (xmax = 0) AS inserted
                """,
                idempotencyKey, candidateTransactionId, Timestamp.from(now), Timestamp.from(now.plus(POSTGRES_TTL)));

        UUID winningTransactionId = (UUID) row.get("transaction_id");
        boolean wasNewlyInserted = (boolean) row.get("inserted");

        String cacheKey = CACHE_KEY_PREFIX + idempotencyKey;
        String cacheValue = winningTransactionId.toString();

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            cacheWrite(cacheKey, cacheValue);
                        }
                    });
        } else {
            cacheWrite(cacheKey, cacheValue);
        }

        return wasNewlyInserted
                ? IdempotencyResult.reserved(winningTransactionId)
                : IdempotencyResult.duplicate(winningTransactionId);
    }

    private void cacheWrite(String key, String value) {
        try {
            redisTemplate.opsForValue().set(key, value, REDIS_TTL);
        } catch (RedisConnectionFailureException e) {
            log.warn(
                    "Redis unavailable for idempotency cache write-through; PostgreSQL row is still authoritative",
                    e);
        }
    }
}
