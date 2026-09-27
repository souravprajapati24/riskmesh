package com.riskmesh.ingestion.repository;

import com.riskmesh.ingestion.outbox.OutboxEvent;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxEventRepository {

    private final JdbcTemplate jdbcTemplate;

    public OutboxEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(UUID aggregateId, String topic, String eventKey, String payloadJson, Instant now) {
        jdbcTemplate.update("""
                INSERT INTO outbox_events (aggregate_id, topic, event_key, payload, created_at)
                VALUES (?, ?, ?, ?::jsonb, ?)
                """, aggregateId, topic, eventKey, payloadJson, Timestamp.from(now));
    }

    public List<OutboxEvent> findUnpublishedBatchForUpdateSkipLocked(int limit) {
        return jdbcTemplate.query("""
                SELECT id, aggregate_id, topic, event_key, payload, created_at, published_at
                FROM outbox_events
                WHERE published_at IS NULL
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT ?
                """, this::mapRow, limit);
    }

    public void markPublished(long id, Instant now) {
        jdbcTemplate.update("UPDATE outbox_events SET published_at = ? WHERE id = ?", Timestamp.from(now), id);
    }

    private OutboxEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp publishedAt = rs.getTimestamp("published_at");
        return new OutboxEvent(
                rs.getLong("id"),
                (UUID) rs.getObject("aggregate_id"),
                rs.getString("topic"),
                rs.getString("event_key"),
                rs.getString("payload"),
                rs.getTimestamp("created_at").toInstant(),
                publishedAt != null ? publishedAt.toInstant() : null);
    }
}
