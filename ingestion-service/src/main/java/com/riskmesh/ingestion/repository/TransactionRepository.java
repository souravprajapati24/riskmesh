package com.riskmesh.ingestion.repository;

import com.riskmesh.ingestion.domain.Transaction;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class TransactionRepository {

    private final JdbcTemplate jdbcTemplate;

    public TransactionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(Transaction txn) {
        jdbcTemplate.update("""
                INSERT INTO transactions (
                    transaction_id, external_txn_id, merchant_id, payer_id, payee_id,
                    amount, currency, payment_method, card_bin, card_last4,
                    device_fingerprint, ip_address, geo_country, geo_city,
                    merchant_category, user_agent, status, received_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::inet, ?, ?, ?, ?, ?, ?, ?)
                """,
                txn.transactionId(), txn.externalTxnId(), txn.merchantId(), txn.payerId(), txn.payeeId(),
                txn.amount(), txn.currency(), txn.paymentMethod(), txn.cardBin(), txn.cardLast4(),
                txn.deviceFingerprint(), txn.ipAddress(), txn.geoCountry(), txn.geoCity(),
                txn.merchantCategory(), txn.userAgent(), txn.status(),
                Timestamp.from(txn.receivedAt()), Timestamp.from(txn.receivedAt()));
    }

    public Optional<String> findStatus(UUID transactionId) {
        List<String> results = jdbcTemplate.query(
                "SELECT status FROM transactions WHERE transaction_id = ?",
                (rs, rowNum) -> rs.getString("status"),
                transactionId);
        return results.stream().findFirst();
    }
}
