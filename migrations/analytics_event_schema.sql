-- ============================================================
--   Phase 2C — Analytics Event Log Table
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics_events (
    id SERIAL PRIMARY KEY,

    event_type TEXT NOT NULL,

    session_id TEXT NOT NULL,
    device_id TEXT NOT NULL,

    user_id INTEGER REFERENCES users(id),

    metadata JSONB,

    child_age INTEGER,
    child_needs TEXT,
    diagnosis TEXT,
    preferred_services TEXT,

    intent_score NUMERIC,

    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_user_id ON analytics_events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_session ON analytics_events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_device ON analytics_events(device_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON analytics_events(event_type);
