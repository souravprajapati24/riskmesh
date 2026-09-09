CREATE TABLE outbox_events (
                               id            BIGSERIAL PRIMARY KEY,
                               aggregate_id  UUID NOT NULL,
                               topic         TEXT NOT NULL,
                               event_key     TEXT NOT NULL,
                               payload       JSONB NOT NULL,
                               created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                               published_at  TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox_events(created_at) WHERE published_at IS NULL;