CREATE TABLE IF NOT EXISTS sensor_readings (
                                               device_id TEXT NOT NULL,
                                               metric_type TEXT NOT NULL,
                                               value DOUBLE PRECISION NOT NULL,
                                               reading_time TIMESTAMPTZ NOT NULL
);

SELECT create_hypertable('sensor_readings', 'reading_time', if_not_exists => TRUE);