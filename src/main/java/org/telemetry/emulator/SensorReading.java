package org.telemetry.emulator;

public record SensorReading(String deviceId, long timestamp, String metricType, double value) {}