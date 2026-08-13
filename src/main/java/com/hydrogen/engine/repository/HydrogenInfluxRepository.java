package com.hydrogen.engine.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hydrogen.engine.domain.TelemetryRecord;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.InfluxDBClientReactiveFactory;
import com.influxdb.client.reactive.WriteReactiveApi;
import com.influxdb.client.write.Point;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Repository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

@Repository
@EnableScheduling // Включаем поддержку scheduler для фонового восстановления данных
public class HydrogenInfluxRepository {

    @Value("${industrial.influxdb.url}") private String influxUrl;
    @Value("${industrial.influxdb.token}") private String token;
    @Value("${industrial.influxdb.org}") private String org;
    @Value("${industrial.influxdb.bucket}") private String bucket;

    private InfluxDBClientReactive influxClient;
    private WriteReactiveApi writeApi;

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Очередь для бэкапа в Valkey
    private static final String FALLBACK_KEY = "conveyor:fallback:queue";
    // Флаг для предотвращения одновременного запуска нескольких процессов восстановления
    private final AtomicBoolean isRecovering = new AtomicBoolean(false);

    private final Sinks.Many<TelemetryRecord> bufferSink = Sinks.many().multicast().onBackpressureBuffer();
    private Disposable batchSubscription;

    // Внедряем StringRedisTemplate, который был настроили для Virtual Threads
    public HydrogenInfluxRepository(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // Переменная для управления симуляцией сбоев в тестах
    private boolean simulateFailure = false;


    @PostConstruct
    public void init() {
        this.influxClient = InfluxDBClientReactiveFactory.create(influxUrl, token.toCharArray(), org, bucket);
        this.writeApi = influxClient.getWriteReactiveApi();
        System.out.println("Реактивный клиент InfluxDB успешно инициализирован.");

        // конвейер: пачки по 10 000 точек или сброс каждые 500 миллисекунд
        this.batchSubscription = bufferSink.asFlux()
                .publishOn(Schedulers.boundedElastic())
                .bufferTimeout(10000, Duration.ofMillis(500))
                .subscribe(
                        new Consumer<List<TelemetryRecord>>() {
                            @Override
                            public void accept(List<TelemetryRecord> records) {
                                flushBatchToInflux(records);
                            }
                        },
                        new Consumer<Throwable>() {
                            @Override
                            public void accept(Throwable error) {
                                System.err.println("Критическая ошибка конвейера буферизации: " + error.getMessage());
                            }
                        }
                );
    }
    /**
     * мгновенный и неблокирующий прием записи из виртуальных потоков приемника
     */

    public void save(TelemetryRecord record) {
        bufferSink.tryEmitNext(record);
    }


    /**
     * основной метод записи пакета в InfluxDB с логикой Circuit Breaker
     * модифицируем метод отправки пачки, чтобы он учитывал симуляцию аварии
     *
     */
    public void flushBatchToInflux(List<TelemetryRecord> records) {
        if (records.isEmpty()) return;

        if (simulateFailure) {
            sendToValkeyFallback(records);
            return;
        }

        List<Point> points = mapToPoints(records);

        Mono.from(writeApi.writePoints(bucket, org, WritePrecision.MS, Flux.fromIterable(points)))
                .doOnSuccess(v -> System.out.println(" Успешно записан пакет из " + records.size() + " точек в InfluxDB."))
                .doOnError(error -> {
                    System.err.println(" Ошибка InfluxDB! Активация буфера Valkey (Circuit Breaker): " + error.getMessage());
                    sendToValkeyFallback(records);
                })
                .subscribe();
    }

    /**
     * Сериализация пакета данных и мгновенное сохранение в Valkey
     * добавление в "голову" списка
     */
    private void sendToValkeyFallback(List<TelemetryRecord> records) {
        try {
            String jsonBatch = objectMapper.writeValueAsString(records);
            // "LPUSH" добавляет пакет в начало списка Valkey
            redisTemplate.opsForList().leftPush(FALLBACK_KEY, jsonBatch);
            System.out.printf("[Circuit Breaker] пакет из %d записей успешно перемещён в Valkey.%n", records.size());
        } catch (JsonProcessingException e) {
            System.err.println("Критическая ошибка сериализации backup: " + e.getMessage());
        }
    }

    /**
     * Планировщик восстановления данных (Запускается в Virtual Thread благодаря Spring Boot настройкам)
     * Каждые 5 секунд проверяет наличие данных в аварийной очереди Valkey
     * забирает пачки из ХВОСТА списка Valkey  (FIFO)
     * и синхронно блокирует виртуальный поток выполнения при отправке в оживающий InfluxDB
     */
    @Scheduled(fixedDelay = 5000)
    public void tryRecovery() {


        // если процесс восстановления уже идет, или в Valkey пусто - выходим
        Long queueSize = redisTemplate.opsForList().size(FALLBACK_KEY);
        if (queueSize == null || queueSize == 0 || !isRecovering.compareAndSet(false, true)) {
            return;
        }

        System.out.printf("[RECOVERY] Обнаружено аварийных пакетов в Valkey: %d. Попытка восстановления...%n", queueSize);

        try {
            // вычитываем пакет с конца списка (атомарно удаляя её из Valkey)
            String jsonBatch = redisTemplate.opsForList().rightPop(FALLBACK_KEY);

            while (jsonBatch != null) {
                // Десериализуем обратно в Java List
                List<TelemetryRecord> records = objectMapper.readValue(
                        jsonBatch,
                        objectMapper.getTypeFactory().constructCollectionType(List.class, TelemetryRecord.class)
                );

                List<Point> points = mapToPoints(records);

                try {
                    // Синхронно через .block() (для текущего виртуального потока шедулера) пытаемся записать в InfluxDB
                    Mono.from(writeApi.writePoints(bucket, org, WritePrecision.MS,
                            reactor.core.publisher.Flux.fromIterable(points))).block(Duration.ofSeconds(10));
                    System.out.printf("[RECOVERY] пакет из %d точек успешно восстановлен в InfluxDB.%n", records.size());
                } catch (Exception influxError) {
                    // Если InfluxDB все еще лежит - возвращаем пакет обратно в Valkey и прекращаем итерацию
                    redisTemplate.opsForList().rightPush(FALLBACK_KEY, jsonBatch);
                    System.err.println("[RECOVERY] InfluxDB все еще недоступен. Восстановление приостановлено.");
                    break;
                }

                // читаем следующий пакет, если он есть
                jsonBatch = redisTemplate.opsForList().rightPop(FALLBACK_KEY);
            }
        } catch (Exception e) {
            System.err.println("Ошибка в процессе восстановления данных: " + e.getMessage());
        } finally {
            isRecovering.set(false); // Освобождаем лок
        }
    }

    private List<Point> mapToPoints(List<TelemetryRecord> records) {
        return records
                .stream()
                .map(new Function<TelemetryRecord, Point>() {
                                        @Override
                                        public Point apply(TelemetryRecord record) {
                                            return Point.measurement("hydrogen_cell_telemetry")
                                                    .addTag("cell_id", record.cell_id())
                                                    .addField("h2_pressure_bar", record.h2_pressure_bar())
                                                    .addField("air_pressure_bar", record.air_pressure_bar())
                                                    .addField("temperature_celsius", record.temperature_celsius())
                                                    .addField("current_load_amper", record.current_load_amper())
                                                    .time(Instant.ofEpochMilli(record.timestamp()), WritePrecision.MS);
                                        }
                                    }
        ).toList();
    }

    /**
     * Технологический метод для сброса состояния Circuit Breaker в интеграционных тестах.
     */
    public void resetRecoveryStateForTest() {
        this.isRecovering.set(false);
        this.simulateFailure = false;
    }

    public void setSimulateFailure(boolean simulateFailure) {
        this.simulateFailure = simulateFailure;
    }



    @PreDestroy
    public void close() {
        if (batchSubscription != null) batchSubscription.dispose();
        if (influxClient != null) {
            influxClient.close();
            System.out.println(" Соединение с InfluxDB безопасно закрыто.");
        }
    }
}
