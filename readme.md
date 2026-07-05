# Reactive IoT Telemetry Platform

Реактивная система мониторинга метрик для сценария "Умный завод". Датчики (температура, давление, вибрация) непрерывно поссылают
показания через Kafka. Сервис на Spring WebFlux читает поток без блокирующих операций, пишет данные в TimescaleDB, детектирует 
аномалии по паттерну CEP и отдает дашборд через SSE.

## Содержание

- [Стек](#стек)
- [Архитектура](#архитектура)
- [Структура проекта](#структура-проекта)
- [Запуск](#запуск)
- [Kafka topics](#kafka-topics)
- [Backpressure и известные проблемы](#backpressure-и-известные-проблемы)
- [Нагрузочный тест](#нагрузочный-тест)

## Стек

| Компонент | Технология |
|---|---|
| Язык | Java 21, records, virtual threads |
| Веб-фреймворк | Spring WebFlux (Netty) |
| Брокер сообщений | Apache Kafka 3.8.0, KRaft, 1 брокер |
| Kafka клиент | Reactor Kafka 1.3.23 |
| База данных | TimescaleDB (PostgreSQL 16) |
| Доступ к БД | R2DBC, без блокирующего JDBC |
| UI для Kafka | kafka-ui |
| Фронтенд | HTML + Chart.js, обновление через Server-Sent Events |
| Сборка | Gradle Kotlin DSL |

## Архитектура

Один поток данных проходит через систему без блокировок на каждом шаге.

1. `SensorEmulatorApp` создаёт виртуальный поток на каждое устройство (`Executors.newVirtualThreadPerTaskExecutor()`) и отправляет JSON-показания в топик `sensors.raw` через `KafkaProducer`.
2. `SensorIngestionService` подписан на `sensors.raw` через `KafkaReceiver`. На каждую запись параллельно выполняются три действия: запись в БД, проверка на аномалию, публикация в SSE-поток.
3. `SensorStorageWriter` собирает показания в батчи (`bufferTimeout(1000, 2s)`) и пишет их в TimescaleDB через `R2dbcEntityTemplate`, с ограничением параллельности вставки до 20 одновременных соединений.
4. `AnomalyDetector` хранит по каждому устройству скользящее окно последних перегревов (`TEMP > 90`). Если фиксируется 3 события за 30 секунд, генерируется `CRITICAL_OVERHEAT` и публикуется в топик `alerts` через `KafkaSender`.
5. `TelemetryBroadcaster` держит два `Sinks.Many`, для показаний и для алертов. Поток показаний прорежен через `sample(150ms)`, чтобы не перегружать рендер графика в браузере.
6. `TelemetryStreamController` отдаёт оба потока как `text/event-stream`. Страница `index.html` подключается через `EventSource` и рисует график в реальном времени без поллинга.

## Структура проекта

```
Reactive_IoT_Telemetry_Platform/
├── build.gradle.kts
├── settings.gradle.kts
├── docker-compose.yml
├── docker/
│   └── init-db/
│       └── 001-init.sql
└── src/main/
    ├── java/org/telemetry/
    │   ├── TelemetryApplication.java
    │   ├── emulator/
    │   │   ├── SensorReading.java
    │   │   └── SensorEmulatorApp.java
    │   ├── ingestion/
    │   │   ├── KafkaReceiverConfig.java
    │   │   └── SensorIngestionService.java
    │   ├── storage/
    │   │   ├── SensorReadingEntity.java
    │   │   ├── SensorReadingRepository.java
    │   │   └── SensorStorageWriter.java
    │   ├── analytics/
    │   │   ├── AlertEvent.java
    │   │   ├── KafkaSenderConfig.java
    │   │   └── AnomalyDetector.java
    │   └── web/
    │       ├── TelemetryBroadcaster.java
    │       └── TelemetryStreamController.java
    └── resources/
        ├── application.yml
        └── static/
            └── index.html
```

`SensorEmulatorApp` и `TelemetryApplication` это две независимые точки входа в одном Gradle-модуле. Первая только пишет в Kafka, вторая поднимает весь остальной пайплайн.

## Запуск

Требования: Docker Desktop, JDK 21, IntelliJ IDEA.

```
docker compose up -d
docker compose ps
```

Поднимаются три контейнера: `kafka`, `kafka-ui`, `timescaledb`. Таблица `sensor_readings` создаётся автоматически из `docker/init-db/001-init.sql` при первой инициализации тома `timescaledb`.

Запустить эмулятор датчиков:

```
./gradlew runEmulator
```

Запустить основной сервис:

```
./gradlew bootRun
```

После старта доступно:

- Дашборд: http://localhost:8080
- Kafka UI: http://localhost:8081
- TimescaleDB: `localhost:5432`, база `telemetry`, пользователь `telemetry`

Проверка количества записанных строк:

```
docker exec -it timescaledb psql -U telemetry -d telemetry -c "SELECT count(*) FROM sensor_readings;"
```

Проверка состояния consumer group:

```
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ingestion-service
```

## Kafka topics

| Топик | Партиции | Repl. factor | Назначение |
|---|---|---|---|
| `sensors.raw` | 1-6 | 1 | показания датчиков |
| `alerts` | 1 | 1 | события `CRITICAL_OVERHEAT` от `AnomalyDetector` |
| `__consumer_offsets` | 50 | 1 | offset consumer-групп |

Количество партиций `sensors.raw` увеличено до 6 перед нагрузочным тестом:

```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic sensors.raw
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors.raw --partitions 6 --replication-factor 1
```

## Нагрузочный тест

Параметры прогона: 5000 виртуальных устройств, интервал отправки 100-300 мс на устройство, топик `sensors.raw` с 6 партициями.

Состояние Kafka после прогона:

| Топик | Партиции | Количество сообщений | Размер |
|---|---|---|---|
| `sensors.raw` | 6 | 890 705 | 104 MB |
| `alerts` | 1 | 554 | 113 KB |
| `__consumer_offsets` | 50 | 9 | 1 KB |
