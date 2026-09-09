CREATE TABLE deduplication_records (
                                       idempotency_key   TEXT PRIMARY KEY,
                                       transaction_id    UUID NOT NULL,
                                       first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                       ttl_expires_at    TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '72 hours'
);

CREATE INDEX idx_dedup_expires ON deduplication_records(ttl_expires_at);