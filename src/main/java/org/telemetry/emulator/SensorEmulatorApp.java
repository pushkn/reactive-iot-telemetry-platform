package org.telemetry.emulator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class SensorEmulatorApp {

    private static final String TOPIC = "sensors.raw";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int DEVICE_COUNT = 5000;
    private static final List<String> METRIC_TYPES = List.of("TEMP", "PRESSURE", "VIBRATION");

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper mapper = new ObjectMapper();
        KafkaProducer<String, String> producer = createProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Останавливаем эмулятор, закрываем producer...");
            producer.flush();
            producer.close();
        }));

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 1; i <= DEVICE_COUNT; i++) {
                String deviceId = "device-" + i;
                executor.submit(() -> runDeviceLoop(deviceId, producer, mapper));
            }
            Thread.currentThread().join();
        }
    }

    private static void runDeviceLoop(String deviceId, KafkaProducer<String, String> producer, ObjectMapper mapper) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                String metricType = METRIC_TYPES.get(ThreadLocalRandom.current().nextInt(METRIC_TYPES.size()));
                double value = generateValue(metricType);

                SensorReading reading = new SensorReading(deviceId, Instant.now().toEpochMilli(), metricType, value);
                String json = mapper.writeValueAsString(reading);

                producer.send(new ProducerRecord<>(TOPIC, deviceId, json), (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Ошибка отправки для " + deviceId + ": " + exception.getMessage());
                    }
                });

                TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(100, 300));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.err.println("Ошибка в цикле датчика " + deviceId + ": " + e.getMessage());
            }
        }
    }

    private static double generateValue(String metricType) {
        double base = switch (metricType) {
            case "TEMP" -> 60.0;
            case "PRESSURE" -> 5.0;
            case "VIBRATION" -> 1.0;
            default -> 0.0;
        };

        boolean anomaly = "TEMP".equals(metricType) && ThreadLocalRandom.current().nextInt(100) < 2;
        double noise = ThreadLocalRandom.current().nextDouble(-2, 2);
        return anomaly ? 95 + ThreadLocalRandom.current().nextDouble(0, 10) : base + noise;
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        return new KafkaProducer<>(props);
    }
}