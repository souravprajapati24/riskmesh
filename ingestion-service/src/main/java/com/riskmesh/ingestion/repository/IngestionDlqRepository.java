package com.riskmesh.ingestion.repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;


@Repository
public class IngestionDlqRepository {

    private final JdbcTemplate jdbcTemplate;

    public IngestionDlqRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void recordFailure(
            UUID transactionId,
            String payloadJson,
            String errorReason,
            Instant now) {

        int updated = jdbcTemplate.update("""
                UPDATE ingestion_dlq
                SET retry_count = retry_count + 1,
                    last_retry_at = ?,
                    error_reason = ?
                WHERE transaction_id = ?
                  AND payload = ?::jsonb
                  AND resolved = FALSE
                """,
                Timestamp.from(now),
                errorReason,
                transactionId,
                payloadJson);

        if (updated == 0) {
            insert(transactionId, payloadJson, errorReason, now);
        }
    }


    public void insert(UUID transactionId, String payloadJson, String errorReason, Instant now) {
        jdbcTemplate.update("""
                INSERT INTO ingestion_dlq (transaction_id, payload, error_reason, created_at)
                VALUES (?, ?::jsonb, ?, ?)
                """, transactionId, payloadJson, errorReason, Timestamp.from(now));
    }
}
