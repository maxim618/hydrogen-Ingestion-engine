package com.hydrogen.engine.telemetry;

import com.influxdb.client.write.WriteParameters;
import reactor.core.publisher.Flux;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.reactive.InfluxDBClientReactive;
import com.influxdb.client.reactive.InfluxDBClientReactiveFactory;
import com.influxdb.client.reactive.WriteReactiveApi;
import com.influxdb.client.write.Point;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Repository
public class HydrogenInfluxRepository {

    // Инжектируем те самые подсвеченные желтым свойства из application.properties
    @Value("${industrial.influxdb.url}")
    private String influxUrl;

    @Value("${industrial.influxdb.token}")
    private String token;

    @Value("${industrial.influxdb.org}")
    private String org;

    @Value("${industrial.influxdb.bucket}")
    private String bucket;

    private InfluxDBClientReactive influxClient;
    private WriteReactiveApi writeApi;

    /**
     * Инициализация подключения после того, как Spring создаст этот класс
     */
    @PostConstruct
    public void init() {
        // Создаем неблокирующий реактивный клиент InfluxDB
        this.influxClient = InfluxDBClientReactiveFactory.create(influxUrl, token.toCharArray(), org, bucket);
        this.writeApi = influxClient.getWriteReactiveApi();
        System.out.println("💾 Реактивный клиент InfluxDB успешно инициализирован и готов к записи пакетов.");
    }

    /**
     * Высокоэффективная неблокирующая запись точки телеметрии
     */
    public void save(TelemetryRecord record) {
        // 1. Формируем точку данных
        Point point = Point.measurement("hydrogen_cell_telemetry")
                .addTag("cell_id", record.cell_id())
                .addField("h2_pressure_bar", record.h2_pressure_bar())
                .addField("air_pressure_bar", record.air_pressure_bar())
                .addField("temperature_celsius", record.temperature_celsius())
                .addField("current_load_amper", record.current_load_amper())
                .time(java.time.Instant.ofEpochMilli(record.timestamp()), WritePrecision.MS);

        // 2. ИСПРАВЛЕНИЕ: Оборачиваем Publisher от InfluxDB в Mono из Project Reactor
        Mono.from(writeApi.writePoint(bucket, org, WritePrecision.MS, point))
                .doOnError(error -> System.err.println("❌ Ошибка реактивной записи в InfluxDB: " + error.getMessage()))
                .subscribe(); // Асинхронный запуск в фоне
    }





    /**
     * Безопасное закрытие соединений при остановке приложения
     */
    @PreDestroy
    public void close() {
        if (influxClient != null) {
            influxClient.close();
            System.out.println("💾 Соединение с InfluxDB безопасно закрыто.");
        }
    }
}
