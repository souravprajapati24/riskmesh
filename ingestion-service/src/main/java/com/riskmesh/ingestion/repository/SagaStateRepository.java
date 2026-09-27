package com.riskmesh.ingestion.repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;


@Repository
public class SagaStateRepository {

    private final JdbcTemplate jdbcTemplate;

    public SagaStateRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertInitiated(UUID transactionId, Instant now) {
        jdbcTemplate.update("""
                INSERT INTO saga_states (transaction_id, current_step, created_at, updated_at)
                VALUES (?, 'INITIATED', ?, ?)
                """, transactionId, Timestamp.from(now), Timestamp.from(now));
    }
}
