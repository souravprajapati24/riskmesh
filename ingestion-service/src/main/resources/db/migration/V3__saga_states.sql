CREATE TABLE saga_states (
                             saga_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                             transaction_id        UUID NOT NULL UNIQUE REFERENCES transactions(transaction_id),
                             current_step           TEXT NOT NULL DEFAULT 'INITIATED'
                                 CHECK (current_step IN ('INITIATED','HOLD_REQUESTED','HOLD_CONFIRMED','SCORING_COMPLETE',
                                                         'COMPLETED','COMPENSATING','COMPENSATED','FAILED')),
                             hold_amount             NUMERIC(20,4),
                             hold_reference           TEXT,
                             created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                             updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                             failure_reason               TEXT,
                             compensation_steps             JSONB NOT NULL DEFAULT '[]'::jsonb,
                             version                          INTEGER NOT NULL DEFAULT 0,
                             step_up_deadline                    TIMESTAMPTZ
);

CREATE INDEX idx_saga_stalled ON saga_states(updated_at)
    WHERE current_step NOT IN ('COMPLETED','COMPENSATED','FAILED');
CREATE INDEX idx_saga_step_up_deadline ON saga_states(step_up_deadline) WHERE step_up_deadline IS NOT NULL;