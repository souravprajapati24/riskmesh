CREATE TABLE risk_decisions (
                                decision_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                transaction_id          UUID NOT NULL UNIQUE,
                                risk_score               DOUBLE PRECISION NOT NULL CHECK (risk_score BETWEEN 0 AND 1),
                                risk_band                 TEXT NOT NULL CHECK (risk_band IN ('LOW','MEDIUM','HIGH','CRITICAL')),
                                decision                    TEXT NOT NULL CHECK (decision IN ('APPROVE','DECLINE','STEP_UP')),
                                scoring_mode                  TEXT NOT NULL CHECK (scoring_mode IN ('FULL_AI','RULES_ONLY_FALLBACK')),
                                rule_signals                    JSONB,
                                similarity_score                  DOUBLE PRECISION,
                                nearest_fraud_cluster                TEXT,
                                ai_narrative                           TEXT,
                                scoring_duration_ms                       BIGINT,
                                decided_at                                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                analyst_review_status                          TEXT NOT NULL DEFAULT 'NOT_REQUIRED'
                                    CHECK (analyst_review_status IN ('NOT_REQUIRED','PENDING','REVIEWED')),
                                analyst_id                                        UUID,
                                analyst_note                                        TEXT,
                                analyst_decision                                       TEXT
                                    CHECK (analyst_decision IS NULL OR analyst_decision IN ('CONFIRMED_FRAUD','CONFIRMED_LEGITIMATE','ESCALATED')),
                                reviewed_at                                              TIMESTAMPTZ
);

CREATE INDEX idx_rd_decision ON risk_decisions(decision);
CREATE INDEX idx_rd_risk_band ON risk_decisions(risk_band);
CREATE INDEX idx_rd_decided_at ON risk_decisions(decided_at DESC);
CREATE INDEX idx_rd_analyst_status ON risk_decisions(analyst_review_status)
    WHERE analyst_review_status = 'PENDING';