-- PostgreSQL analytics schema for Gold layer data

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.daily_events (
    event_date      DATE            NOT NULL,
    region          VARCHAR(64)     NOT NULL,
    category        VARCHAR(64)     NOT NULL,
    total_events    BIGINT          DEFAULT 0,
    total_revenue   NUMERIC(18,2)   DEFAULT 0,
    avg_order       NUMERIC(18,4)   DEFAULT 0,
    unique_users    BIGINT          DEFAULT 0,
    anomaly_count   BIGINT          DEFAULT 0,
    loaded_at       TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (event_date, region, category)
);

CREATE TABLE IF NOT EXISTS analytics.user_profiles (
    user_id         VARCHAR(64)     PRIMARY KEY,
    total_orders    BIGINT          DEFAULT 0,
    lifetime_value  NUMERIC(18,2)   DEFAULT 0,
    avg_order       NUMERIC(18,4)   DEFAULT 0,
    last_seen       TIMESTAMP,
    is_churned      BOOLEAN         DEFAULT FALSE,
    updated_at      TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX idx_daily_date    ON analytics.daily_events(event_date);
CREATE INDEX idx_daily_region  ON analytics.daily_events(region);
CREATE INDEX idx_user_churn    ON analytics.user_profiles(is_churned);
CREATE INDEX idx_user_ltv      ON analytics.user_profiles(lifetime_value DESC);

-- Aggregation view for dashboards
CREATE OR REPLACE VIEW analytics.revenue_summary AS
SELECT
    region,
    SUM(total_revenue)  AS total_revenue,
    SUM(total_events)   AS total_events,
    AVG(avg_order)      AS avg_order,
    SUM(anomaly_count)  AS total_anomalies
FROM analytics.daily_events
GROUP BY region
ORDER BY total_revenue DESC;
