package org.telemetry.analytics;

public record AlertEvent(String deviceId, String metricType, String alertType, double value, long timestamp) {}