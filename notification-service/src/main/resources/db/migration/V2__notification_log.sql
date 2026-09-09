CREATE TABLE notification_log (
                                  notification_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                  transaction_id      UUID NOT NULL,
                                  channel               TEXT NOT NULL CHECK (channel IN ('OTP_SMS','OTP_EMAIL','MERCHANT_ALERT')),
                                  status                  TEXT NOT NULL CHECK (status IN ('SENT','FAILED','DELIVERED')),
                                  provider_response          TEXT,
                                  sent_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notif_txn ON notification_log(transaction_id);