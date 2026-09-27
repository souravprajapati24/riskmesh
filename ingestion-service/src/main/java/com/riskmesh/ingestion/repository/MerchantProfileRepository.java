package com.riskmesh.ingestion.repository;

import com.riskmesh.ingestion.domain.MerchantProfile;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MerchantProfileRepository {

    private final JdbcTemplate jdbcTemplate;

    public MerchantProfileRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<MerchantProfile> findById(UUID merchantId) {
        List<MerchantProfile> results = jdbcTemplate.query("""
                SELECT merchant_id, merchant_name, mcc, risk_tier, max_transaction_limit,
                       velocity_window_sec, max_velocity_count, step_up_threshold, decline_threshold, active
                FROM merchant_profiles WHERE merchant_id = ?
                """, this::mapRow, merchantId);
        return results.stream().findFirst();
    }

    private MerchantProfile mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new MerchantProfile(
                (UUID) rs.getObject("merchant_id"),
                rs.getString("merchant_name"),
                rs.getString("mcc"),
                rs.getString("risk_tier"),
                rs.getBigDecimal("max_transaction_limit"),
                rs.getInt("velocity_window_sec"),
                rs.getInt("max_velocity_count"),
                rs.getDouble("step_up_threshold"),
                rs.getDouble("decline_threshold"),
                rs.getBoolean("active"));
    }
}
