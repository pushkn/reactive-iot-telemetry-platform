package org.telemetry.web;

import org.springframework.stereotype.Component;
import org.telemetry.analytics.AlertEvent;
import org.telemetry.emulator.SensorReading;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;

@Component
public class TelemetryBroadcaster {

    private final Sinks.Many<SensorReading> readingsSink = Sinks.many().multicast().onBackpressureBuffer();
    private final Sinks.Many<AlertEvent> alertsSink = Sinks.many().multicast().onBackpressureBuffer();

    public void publishReading(SensorReading reading) {
        readingsSink.tryEmitNext(reading);
    }

    public void publishAlert(AlertEvent alert) {
        alertsSink.tryEmitNext(alert);
    }

    public Flux<SensorReading> readings() {
        return readingsSink.asFlux().sample(Duration.ofMillis(150));
    }

    public Flux<AlertEvent> alerts() {
        return alertsSink.asFlux();
    }
}