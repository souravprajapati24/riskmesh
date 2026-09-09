CREATE TABLE fraud_patterns (
                                pattern_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                embedding           vector(1536) NOT NULL,
                                pattern_label       TEXT NOT NULL,
                                feature_description TEXT NOT NULL,
                                example_count       INTEGER NOT NULL DEFAULT 1,
                                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                active              BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX fraud_patterns_ivfflat ON fraud_patterns
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);