CREATE TABLE merchant_profiles (
                                   merchant_id            UUID PRIMARY KEY,
                                   merchant_name          TEXT NOT NULL,
                                   mcc                     TEXT NOT NULL,
                                   risk_tier                TEXT NOT NULL DEFAULT 'LOW' CHECK (risk_tier IN ('LOW','MEDIUM','HIGH')),
                                   max_transaction_limit      NUMERIC(20,4),
                                   velocity_window_sec           INTEGER NOT NULL DEFAULT 60,
                                   max_velocity_count               INTEGER NOT NULL DEFAULT 5,
                                   step_up_threshold                   DOUBLE PRECISION NOT NULL DEFAULT 0.55,
                                   decline_threshold                     DOUBLE PRECISION NOT NULL DEFAULT 0.85,
                                   active                                   BOOLEAN NOT NULL DEFAULT TRUE
);