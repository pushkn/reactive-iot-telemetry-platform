package org.telemetry.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;

@Component
public class SensorStorageWriter {

    private static final Logger log = LoggerFactory.getLogger(SensorStorageWriter.class);
    private static final int WRITE_CONCURRENCY = 20;

    private final R2dbcEntityTemplate template;
    private final Sinks.Many<SensorReadingEntity> sink = Sinks.many().unicast().onBackpressureBuffer();

    public SensorStorageWriter(R2dbcEntityTemplate template) {
        this.template = template;
        startBatchWriter();
    }

    public void accept(SensorReadingEntity entity) {
        Sinks.EmitResult result = sink.tryEmitNext(entity);
        if (result.isFailure()) {
            log.warn("Dropped reading due to sink emit failure: {}", result);
        }
    }

    private void startBatchWriter() {
        sink.asFlux()
                .bufferTimeout(1000, Duration.ofSeconds(2))
                .flatMap(this::writeBatch)
                .retry()
                .subscribe();
    }

    private Flux<SensorReadingEntity> writeBatch(List<SensorReadingEntity> batch) {
        if (batch.isEmpty()) {
            return Flux.empty();
        }
        return Flux.fromIterable(batch)
                .flatMap(template::insert, WRITE_CONCURRENCY)
                .doOnComplete(() -> log.info("Wrote batch of {} readings to TimescaleDB", batch.size()))
                .onErrorResume(e -> {
                    log.error("Failed to write batch of {} readings, batch dropped", batch.size(), e);
                    return Flux.empty();
                });
    }
}