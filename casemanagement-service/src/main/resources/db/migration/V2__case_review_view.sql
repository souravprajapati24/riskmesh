CREATE TABLE case_review_view (
                                  transaction_id      UUID PRIMARY KEY,
                                  external_txn_id     TEXT,
                                  merchant_id         UUID,
                                  merchant_name       TEXT,
                                  payer_id            UUID,
                                  amount              NUMERIC(20,4),
                                  currency            CHAR(3),
                                  payment_method      TEXT,
                                  received_at         TIMESTAMPTZ,
                                  decided_at          TIMESTAMPTZ,
                                  risk_score          DOUBLE PRECISION,
                                  risk_band           TEXT,
                                  decision            TEXT,
                                  scoring_mode        TEXT,
                                  rule_signals        JSONB,
                                  similarity_score    DOUBLE PRECISION,
                                  nearest_fraud_cluster TEXT,
                                  ai_narrative        TEXT,
                                  analyst_review_status TEXT DEFAULT 'NOT_REQUIRED',
                                  analyst_id          UUID,
                                  analyst_note        TEXT,
                                  analyst_decision    TEXT,
                                  reviewed_at         TIMESTAMPTZ,
                                  geo_country         TEXT,
                                  geo_city            TEXT,
                                  device_fingerprint  TEXT,
                                  ip_address          TEXT
);

CREATE INDEX idx_crv_review_status ON case_review_view(analyst_review_status);
CREATE INDEX idx_crv_decided_at    ON case_review_view(decided_at DESC);
CREATE INDEX idx_crv_merchant_id   ON case_review_view(merchant_id);
CREATE INDEX idx_crv_risk_band     ON case_review_view(risk_band);