package org.telemetry.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.telemetry.analytics.AnomalyDetector;
import org.telemetry.emulator.SensorReading;
import org.telemetry.storage.SensorReadingEntity;
import org.telemetry.storage.SensorStorageWriter;
import org.telemetry.web.TelemetryBroadcaster;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Instant;

@Component
public class SensorIngestionService {

    private static final Logger log = LoggerFactory.getLogger(SensorIngestionService.class);

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final ObjectMapper objectMapper;
    private final SensorStorageWriter storageWriter;
    private final AnomalyDetector anomalyDetector;
    private final TelemetryBroadcaster broadcaster;

    public SensorIngestionService(KafkaReceiver<String, String> kafkaReceiver,
                                  ObjectMapper objectMapper,
                                  SensorStorageWriter storageWriter,
                                  AnomalyDetector anomalyDetector,
                                  TelemetryBroadcaster broadcaster) {
        this.kafkaReceiver = kafkaReceiver;
        this.objectMapper = objectMapper;
        this.storageWriter = storageWriter;
        this.anomalyDetector = anomalyDetector;
        this.broadcaster = broadcaster;
    }

    @PostConstruct
    public void start() {
        kafkaReceiver.receive()
                .doOnNext(record -> {
                    try {
                        SensorReading reading = objectMapper.readValue(record.value(), SensorReading.class);
                        storageWriter.accept(new SensorReadingEntity(
                                reading.deviceId(),
                                reading.metricType(),
                                reading.value(),
                                Instant.ofEpochMilli(reading.timestamp())
                        ));
                        anomalyDetector.process(reading);
                        broadcaster.publishReading(reading);
                    } catch (Exception e) {
                        log.error("Failed to parse message: {}", record.value(), e);
                    } finally {
                        record.receiverOffset().acknowledge();
                    }
                })
                .doOnError(e -> log.error("Kafka receive stream error", e))
                .subscribe();
    }
}