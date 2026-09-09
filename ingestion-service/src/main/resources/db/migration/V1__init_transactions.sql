CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE transactions (
                              transaction_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                              external_txn_id     TEXT NOT NULL,
                              merchant_id         UUID NOT NULL,
                              payer_id            UUID NOT NULL,
                              payee_id            UUID NOT NULL,
                              amount              NUMERIC(20,4) NOT NULL CHECK (amount > 0),
                              currency             CHAR(3) NOT NULL,
                              payment_method       TEXT NOT NULL CHECK (payment_method IN ('CARD','UPI','NETBANKING','WALLET')),
                              card_bin             TEXT,
                              card_last4           CHAR(4),
                              device_fingerprint   TEXT,
                              ip_address           INET,
                              geo_country           CHAR(2),
                              geo_city               TEXT,
                              merchant_category       TEXT,
                              user_agent               TEXT,
                              status                    TEXT NOT NULL DEFAULT 'RECEIVED'
                                  CHECK (status IN ('RECEIVED','HOLD_REQUESTED','SCORING','SCORED','COMPLETED','FAILED','COMPENSATED')),
                              received_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                              updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                              version                    INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX uq_txn_external ON transactions(external_txn_id, merchant_id);
CREATE INDEX idx_txn_merchant ON transactions(merchant_id);
CREATE INDEX idx_txn_payer ON transactions(payer_id);
CREATE INDEX idx_txn_received_at ON transactions(received_at DESC);
CREATE INDEX idx_txn_status ON transactions(status)
    WHERE status NOT IN ('COMPLETED','COMPENSATED','FAILED');