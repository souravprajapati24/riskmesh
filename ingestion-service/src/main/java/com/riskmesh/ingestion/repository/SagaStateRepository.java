package com.riskmesh.ingestion.repository;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.riskmesh.ingestion.domain.SagaState;
import com.riskmesh.ingestion.domain.enums.SagaStep;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;


@Repository
public class SagaStateRepository {

    private static final String SELECT_COLUMNS = """
            saga_id, transaction_id, current_step, hold_amount, hold_reference, created_at, updated_at,
            failure_reason, compensation_steps::text AS compensation_steps, version, step_up_deadline
            """;
    private static final int STALLED_BATCH_LIMIT = 500;

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

    public Optional<SagaState> findByTransactionId(UUID transactionId) {
        List<SagaState> results = jdbcTemplate.query(
                "SELECT " + SELECT_COLUMNS + " FROM saga_states WHERE transaction_id = ?",
                this::mapRow, transactionId);
        return results.stream().findFirst();
    }

    public int transition(UUID sagaId, SagaStep expectedStep, SagaStep nextStep, int expectedVersion,
                          String holdReference, BigDecimal holdAmount, String failureReason, Instant now) {
        return jdbcTemplate.update("""
                UPDATE saga_states
                SET current_step = ?,
                    version = version + 1,
                    updated_at = ?,
                    hold_reference = COALESCE(?::text, hold_reference),
                    hold_amount = COALESCE(?::numeric, hold_amount),
                    failure_reason = COALESCE(?::text, failure_reason)
                WHERE saga_id = ? AND current_step = ? AND version = ?
                """,
                nextStep.name(), Timestamp.from(now), holdReference, holdAmount, failureReason,
                sagaId, expectedStep.name(), expectedVersion);
    }

    public void appendCompensationSteps(UUID sagaId, String stepsJsonArray, Instant now) {
        jdbcTemplate.update("""
                UPDATE saga_states
                SET compensation_steps = compensation_steps || ?::jsonb, updated_at = ?
                WHERE saga_id = ?
                """, stepsJsonArray, Timestamp.from(now), sagaId);
    }


    public List<SagaState> findStalled(Instant cutoff) {
        return jdbcTemplate.query(
                "SELECT " + SELECT_COLUMNS + """
                        FROM saga_states
                        WHERE current_step IN ('INITIATED', 'HOLD_REQUESTED', 'COMPENSATING')
                          AND updated_at < ?
                        ORDER BY updated_at
                        LIMIT ?
                        """,
                this::mapRow, Timestamp.from(cutoff), STALLED_BATCH_LIMIT);
    }

    private SagaState mapRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp deadline = rs.getTimestamp("step_up_deadline");
        return new SagaState(
                (UUID) rs.getObject("saga_id"),
                (UUID) rs.getObject("transaction_id"),
                SagaStep.valueOf(rs.getString("current_step")),
                rs.getBigDecimal("hold_amount"),
                rs.getString("hold_reference"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(),
                rs.getString("failure_reason"),
                rs.getString("compensation_steps"),
                rs.getInt("version"),
                deadline != null ? deadline.toInstant() : null);
    }


}
