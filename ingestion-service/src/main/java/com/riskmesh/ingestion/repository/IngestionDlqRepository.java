package com.riskmesh.ingestion.repository;

import com.riskmesh.ingestion.outbox.IngestionDlqEntry;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;


@Repository
public class IngestionDlqRepository {

    private final JdbcTemplate jdbcTemplate;

    public IngestionDlqRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(UUID transactionId, String payloadJson, String errorReason, Instant now) {
        jdbcTemplate.update("""
                INSERT INTO ingestion_dlq (transaction_id, payload, error_reason, created_at)
                VALUES (?, ?::jsonb, ?, ?)
                """, transactionId, payloadJson, errorReason, Timestamp.from(now));
    }

    public Optional<UUID> findUnresolvedId(UUID transactionId, String payloadJson) {
        List<UUID> ids = jdbcTemplate.query("""
                SELECT id FROM ingestion_dlq
                WHERE transaction_id = ? AND payload = ?::jsonb AND resolved = FALSE
                LIMIT 1
                """, (rs, rowNum) -> (UUID) rs.getObject("id"), transactionId, payloadJson);
        return ids.stream().findFirst();
    }

    public void incrementRetry(UUID id, String errorReason, Instant now) {
        jdbcTemplate.update("""
                UPDATE ingestion_dlq
                SET retry_count = retry_count + 1, last_retry_at = ?, error_reason = ?
                WHERE id = ?
                """, Timestamp.from(now), errorReason, id);
    }


    public int markResolved(UUID transactionId, String payloadJson, Instant now) {
        return jdbcTemplate.update("""
                UPDATE ingestion_dlq
                SET resolved = TRUE, last_retry_at = ?
                WHERE transaction_id = ? AND payload = ?::jsonb AND resolved = FALSE
                """, Timestamp.from(now), transactionId, payloadJson);
    }

    public List<IngestionDlqEntry> findUnresolvedCreatedBefore(Instant cutoff) {
        return jdbcTemplate.query("""
                SELECT id, transaction_id, error_reason, retry_count, created_at
                FROM ingestion_dlq
                WHERE resolved = FALSE AND created_at < ?
                ORDER BY created_at
                """, this::mapEntry, Timestamp.from(cutoff));
    }

    private IngestionDlqEntry mapEntry(ResultSet rs, int rowNum) throws SQLException {
        return new IngestionDlqEntry(
                (UUID) rs.getObject("id"),
                (UUID) rs.getObject("transaction_id"),
                rs.getString("error_reason"),
                rs.getInt("retry_count"),
                rs.getTimestamp("created_at").toInstant());
    }
}
