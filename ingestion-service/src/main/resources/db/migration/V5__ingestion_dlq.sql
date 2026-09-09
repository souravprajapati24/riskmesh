CREATE TABLE ingestion_dlq (
                               id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                               transaction_id     UUID,
                               payload             JSONB NOT NULL,
                               error_reason          TEXT NOT NULL,
                               created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                               retry_count               INTEGER NOT NULL DEFAULT 0,
                               last_retry_at               TIMESTAMPTZ,
                               resolved                      BOOLEAN NOT NULL DEFAULT FALSE
);