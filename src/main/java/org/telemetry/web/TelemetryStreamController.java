package org.telemetry.web;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.telemetry.analytics.AlertEvent;
import org.telemetry.emulator.SensorReading;
import reactor.core.publisher.Flux;

@RestController
public class TelemetryStreamController {

    private final TelemetryBroadcaster broadcaster;

    public TelemetryStreamController(TelemetryBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @GetMapping(value = "/api/stream/readings", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<SensorReading> streamReadings() {
        return broadcaster.readings();
    }

    @GetMapping(value = "/api/stream/alerts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<AlertEvent> streamAlerts() {
        return broadcaster.alerts();
    }
}