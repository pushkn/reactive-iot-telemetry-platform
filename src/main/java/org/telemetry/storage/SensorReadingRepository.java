package org.telemetry.storage;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface SensorReadingRepository extends ReactiveCrudRepository<SensorReadingEntity, String> {
}