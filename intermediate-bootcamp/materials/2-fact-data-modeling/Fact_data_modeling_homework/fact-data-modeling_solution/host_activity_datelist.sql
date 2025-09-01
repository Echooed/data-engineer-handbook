-- DDL: hosts_cumulated

DROP TABLE IF EXISTS hosts_cumulated;
CREATE TABLE hosts_cumulated (
    host TEXT NOT NULL,
    host_activity_datelist DATE[] NOT NULL,   -- dates with any activity
    snapshot_date DATE NOT NULL,              -- e.g. 2023‑01‑31
    activity_bits BIGINT,                     -- 31‑bit mask for Jan
    PRIMARY KEY (host, snapshot_date)
);

-- Incremental merge for 2023‑01‑31 into hosts_cumulated
WITH deduped AS (
    SELECT
      e.host,
      DATE(e.event_time::DATE) AS activity_date,
      ROW_NUMBER() OVER (PARTITION BY e.host, DATE(e.event_time::DATE)
                         ORDER BY e.event_time) AS rn
    FROM events e
    WHERE e.host IS NOT NULL
),
yesterday AS (
    SELECT host, host_activity_datelist, snapshot_date
    FROM hosts_cumulated
    WHERE snapshot_date = DATE '2023-01-01'
),
today AS (
    SELECT host, activity_date
    FROM deduped
    WHERE rn = 1
      AND activity_date = DATE '2023-01-01'
)

INSERT INTO hosts_cumulated (host, host_activity_datelist, snapshot_date, activity_bits)
SELECT
    COALESCE(t.host, y.host) AS host,
    CASE
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.activity_date]
        WHEN t.activity_date IS NULL THEN y.host_activity_datelist
        WHEN y.host_activity_datelist @> ARRAY[t.activity_date] THEN y.host_activity_datelist
        ELSE ARRAY[t.activity_date] || y.host_activity_datelist
    END AS host_activity_datelist,
    COALESCE(t.activity_date, y.snapshot_date + INTERVAL '1 day')::DATE AS snapshot_date,
    -- compute 31‑bit mask for January from the new datelist
    (
      SELECT COALESCE(SUM(1 << (EXTRACT(DAY FROM d)::INT - 1)), 0)::BIGINT
      FROM unnest(
        CASE
          WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.activity_date]
          WHEN t.activity_date IS NULL THEN y.host_activity_datelist
          WHEN y.host_activity_datelist @> ARRAY[t.activity_date] THEN y.host_activity_datelist
          ELSE ARRAY[t.activity_date] || y.host_activity_datelist
        END
      ) AS d
      WHERE d BETWEEN DATE '2023-01-01' AND DATE '2023-01-31'
    ) AS activity_bits
FROM today t
FULL  JOIN yesterday y USING (host)
ON CONFLICT (host, snapshot_date) DO UPDATE
  SET host_activity_datelist = EXCLUDED.host_activity_datelist,
      activity_bits          = EXCLUDED.activity_bits;



-- DDL: host_activity_reduced

DROP TABLE IF EXISTS host_activity_reduced;
CREATE TABLE host_activity_reduced (
    month DATE        NOT NULL,               -- 2023‑01‑01
    host TEXT        NOT NULL,
    hit_array INTEGER[31],
    unique_visitors_array INTEGER[31],
    total_hits INTEGER,
    total_unique_visitors INTEGER,
    active_days INTEGER,
    activity_bits BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (month, host)
);



-- Build/Upsert host_activity_reduced for January 2023

WITH day_positions AS (
    SELECT generate_series(1,31) AS day_num
),
daily_stats AS (
    SELECT
      e.host,
      EXTRACT(DAY FROM e.event_time::DATE)::INT AS day_num,
      COUNT(*)            AS daily_hits,
      COUNT(DISTINCT e.user_id) AS daily_unique_visitors
    FROM events e
    WHERE e.event_time::DATE
          BETWEEN DATE '2023-01-01' AND DATE '2023-01-31'
      AND e.host IS NOT NULL
    GROUP BY e.host, EXTRACT(DAY FROM e.event_time::DATE)
),
monthly_uv AS (
    SELECT
      e.host,
      COUNT(DISTINCT e.user_id) AS monthly_unique_visitors
    FROM events e
    WHERE e.event_time::DATE
          BETWEEN DATE '2023-01-01' AND DATE '2023-01-31'
      AND e.host IS NOT NULL
    GROUP BY e.host
),
combined AS (
    SELECT
      h.host,
      -- hit_array: real daily_hits[1..31]
      ARRAY(
        SELECT COALESCE(ds.daily_hits, 0)
        FROM day_positions d
        LEFT JOIN daily_stats ds
          ON ds.host = h.host AND ds.day_num = d.day_num
        ORDER BY d.day_num
      ) AS hit_array,
      -- unique_visitors_array
      ARRAY(
        SELECT COALESCE(ds.daily_unique_visitors, 0)
        FROM day_positions d
        LEFT JOIN daily_stats ds
          ON ds.host = h.host AND ds.day_num = d.day_num
        ORDER BY d.day_num
      ) AS unique_visitors_array,
      -- total_hits
      COALESCE(SUM(ds.daily_hits),0) AS total_hits,
      -- total_unique_visitors
      COALESCE(muv.monthly_unique_visitors,0) AS total_unique_visitors
    FROM (
      SELECT DISTINCT host FROM daily_stats
    ) h
    LEFT JOIN daily_stats ds ON ds.host = h.host
    LEFT JOIN monthly_uv muv  ON muv.host = h.host
    GROUP BY h.host, muv.monthly_unique_visitors
)

INSERT INTO host_activity_reduced (
    month, host,
    hit_array, unique_visitors_array,
    total_hits, total_unique_visitors,
    active_days, activity_bits, updated_at
)
SELECT
    DATE '2023-01-01' AS month,
    c.host,
    c.hit_array,
    c.unique_visitors_array,
    c.total_hits,
    c.total_unique_visitors,
    -- active_days from non-zero hit_array entries
    (SELECT COUNT(*) FROM unnest(c.hit_array) v WHERE v > 0) AS active_days,
    -- activity_bits from hosts_cumulated snapshot
    hc.activity_bits,
    CURRENT_TIMESTAMP
FROM combined c
LEFT JOIN hosts_cumulated hc
  ON hc.host = c.host
 AND hc.snapshot_date = DATE '2023-01-31'
ON CONFLICT (month, host) DO UPDATE
  SET hit_array             = EXCLUDED.hit_array,
      unique_visitors_array = EXCLUDED.unique_visitors_array,
      total_hits            = EXCLUDED.total_hits,
      total_unique_visitors = EXCLUDED.total_unique_visitors,
      active_days           = EXCLUDED.active_days,
      activity_bits         = EXCLUDED.activity_bits,
      updated_at            = EXCLUDED.updated_at;



SELECT * FROM host_activity_reduced;