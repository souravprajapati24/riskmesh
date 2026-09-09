CREATE TABLE analyst_users (
                               analyst_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                               username         TEXT NOT NULL UNIQUE,
                               password_hash    TEXT NOT NULL,
                               role             TEXT NOT NULL CHECK (role IN ('ROLE_ANALYST','ROLE_ADMIN')),
                               active           BOOLEAN NOT NULL DEFAULT TRUE,
                               created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);