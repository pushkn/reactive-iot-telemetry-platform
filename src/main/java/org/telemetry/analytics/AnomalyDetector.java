package org.telemetry.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.telemetry.emulator.SensorReading;
import org.telemetry.web.TelemetryBroadcaster;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class AnomalyDetector {

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetector.class);
    private static final double TEMP_THRESHOLD = 90.0;
    private static final int REQUIRED_HITS = 3;
    private static final long WINDOW_MILLIS = 30_000;

    private final KafkaSender<String, String> kafkaSender;
    private final ObjectMapper objectMapper;
    private final String alertsTopic;
    private final TelemetryBroadcaster broadcaster;
    private final ConcurrentHashMap<String, Deque<Long>> overheatWindows = new ConcurrentHashMap<>();

    public AnomalyDetector(KafkaSender<String, String> kafkaSender,
                           ObjectMapper objectMapper,
                           @Value("${kafka.topics.alerts}") String alertsTopic,
                           TelemetryBroadcaster broadcaster) {
        this.kafkaSender = kafkaSender;
        this.objectMapper = objectMapper;
        this.alertsTopic = alertsTopic;
        this.broadcaster = broadcaster;
    }

    public void process(SensorReading reading) {
        if (!"TEMP".equals(reading.metricType()) || reading.value() <= TEMP_THRESHOLD) {
            return;
        }

        Deque<Long> window = overheatWindows.computeIfAbsent(reading.deviceId(), k -> new ArrayDeque<>());

        synchronized (window) {
            window.addLast(reading.timestamp());
            while (!window.isEmpty() && reading.timestamp() - window.peekFirst() > WINDOW_MILLIS) {
                window.pollFirst();
            }

            if (window.size() >= REQUIRED_HITS) {
                window.clear();
                publishAlert(reading);
            }
        }
    }

    private void publishAlert(SensorReading reading) {
        AlertEvent alert = new AlertEvent(reading.deviceId(), reading.metricType(), "CRITICAL_OVERHEAT", reading.value(), reading.timestamp());
        try {
            String json = objectMapper.writeValueAsString(alert);
            SenderRecord<String, String, String> record = SenderRecord.create(
                    new ProducerRecord<>(alertsTopic, reading.deviceId(), json), reading.deviceId());

            kafkaSender.send(reactor.core.publisher.Mono.just(record))
                    .doOnNext(result -> log.warn("Alert published: {}", alert))
                    .doOnError(e -> log.error("Failed to publish alert", e))
                    .subscribe();

            broadcaster.publishAlert(alert);
        } catch (Exception e) {
            log.error("Failed to serialize alert", e);
        }
    }
}