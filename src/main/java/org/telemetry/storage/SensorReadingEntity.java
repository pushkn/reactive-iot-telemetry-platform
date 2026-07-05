package org.telemetry.storage;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Table("sensor_readings")
public record SensorReadingEntity(
        @Id String deviceId,
        String metricType,
        double value,
        Instant readingTime
) {}