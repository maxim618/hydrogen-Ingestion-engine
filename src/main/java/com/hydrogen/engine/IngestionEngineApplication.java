package com.hydrogen.engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IngestionEngineApplication {

    public static void main(String[] args) {

        SpringApplication.run(IngestionEngineApplication.class, args);

        System.out.println("\n_________________________________________________");
        System.out.println(" Hydrogen Ingestion Engine started!");
        System.out.println(" Конвейер подключен к MQTT и реактивной InfluxDB.");
        System.out.println("_____________________________________________________\n");
    }
}
