CREATE TABLE scoring_dlq_events (
                                    dlq_event_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                    original_topic   TEXT NOT NULL,
                                    original_offset  BIGINT NOT NULL,
                                    transaction_id   UUID,
                                    payload          JSONB NOT NULL,
                                    error_class      TEXT NOT NULL,
                                    error_message    TEXT NOT NULL,
                                    failed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                    replayed_at      TIMESTAMPTZ
);

CREATE INDEX idx_dlq_txn ON scoring_dlq_events(transaction_id);
CREATE INDEX idx_dlq_unreplayed ON scoring_dlq_events(failed_at) WHERE replayed_at IS NULL;