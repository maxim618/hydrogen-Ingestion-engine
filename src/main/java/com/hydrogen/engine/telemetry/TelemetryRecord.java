package com.hydrogen.engine.telemetry;

/**
 * Неизменяемая структура данных (DTO) для мгновенного парсинга JSON телеметрии датчиков.
 * Использует синтаксис Java 21 Record.
 */
public record TelemetryRecord(
        long timestamp,
        String cell_id,
        double h2_pressure_bar,
        double air_pressure_bar,
        double temperature_celsius,
        double current_load_amper
) {}
