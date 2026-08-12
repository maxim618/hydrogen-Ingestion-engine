package com.hydrogen.engine.domain;

/**
 * Неизменяемая структура данных для мгновенного парсинга JSON телеметрии датчиков.
 */
public record TelemetryRecord(
        long timestamp,
        String cell_id,
        double h2_pressure_bar,
        double air_pressure_bar,
        double temperature_celsius,
        double current_load_amper
) {}
