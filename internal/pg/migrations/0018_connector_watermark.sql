CREATE TABLE IF NOT EXISTS t_connector_watermark (
    model TEXT PRIMARY KEY,
    write_date TIMESTAMPTZ NOT NULL,
    record_id BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
