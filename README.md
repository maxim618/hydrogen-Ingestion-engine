[![CI](https://github.com/maxim618/hydrogen-Ingestion-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/maxim618/hydrogen-Ingestion-engine/actions/workflows/ci.yml) [![Java](https://img.shields.io/badge/Java-21-ED8B00?logo=openjdk&logoColor=white)](https://www.oracle.com/java/) [![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.0-6DB33F?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot) [![Spring AI](https://img.shields.io/badge/Spring%20AI-2.0.0-6DB33F?logo=spring&logoColor=white)](https://spring.io/projects/spring-ai) [![MQTT](https://img.shields.io/badge/MQTT-Protocol-660066?logo=mqtt&logoColor=white)](https://mqtt.org/) [![InfluxDB](https://img.shields.io/badge/InfluxDB-2.x-22ADF6?logo=influxdb&logoColor=white)](https://www.influxdata.com/) [![Valkey](https://img.shields.io/badge/Valkey-7.x-DC382D?logo=redis&logoColor=white)](https://valkey.io/) [![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

# Hydrogen Ingestion Engine (H2-IE)
Высоконагруженный реактивный движок (Data Ingestion Engine) на Java 21 для сбора, валидации и пакетного сохранения 
телеметрии с датчиков водородных энергетических установок (PEMFC) в режиме реального времени.
##  Архитектура системы
Проект решает классическую проблему промышленного интернета вещей (IIoT) - сбор терабайтов неструктурированных данных с
датчиков без перегрузки серверных мощностей.


      [Водородная Станция]
                │ (миллионы сообщений в сек.)
                ▼
        [MQTT Broker] (Eclipse Mosquitto)
                │
                ▼
        [Java Ingestion Engine] (Spring Boot 4.1.0 + Virtual Threads)
                 ├── 1. Fast-Fail Validator (отсечение невалидных данных на лету)
                 ├── 2. Reactive In-Memory Buffer (накопление пакетов данных)
                 └── 3. Circuit Breaker (защита при отказе БД)
                 │
                 ▼
           [Time-Series DB] (InfluxDB 2.x) ──► визуализация [Grafana Dashboards]


### Ключевые инженерные решения:

1. **Java 21 with virtual threads**:
  использование легковесных виртуальных потоков позволяет обрабатывать сотни тысяч одновременных
  сессий датчиков на минимальных ресурсах CPU без блокировки ядра ОС.
2. **Backpressure & Batching (Пакетная запись)**:
     софт не пишет каждую точку в БД отдельно. Вместо этого реактивный буфер накапливает данные
   (500 мс или 10 000 записей) и сбрасывает их в InfluxDB одним быстрым атомарным запросом
   (Batch Insert), сохраняя ресурс дисков.
3. **Промышленная валидация**: Встроенный слой фильтрации отсекает аномалии "железа" 
   (минусовое давление, null-значения, текстовый мусор) на уровне наносекунд,
    не засоряя базу данных.

##  Технологический стек
* Java 21
* Spring Boot 4.1.0 (Spring Integration MQTT, Spring WebFlux)
* Протокол MQTT (Eclipse Paho Client)
* Базы данных InfluxDB 2.x (Time-Series DB)
* Valkey 7.x
* Docker, Grafana



### Доступы к веб-интерфейсам
* **InfluxDB UI**: `http://localhost:8086`
    * Логин: Значение INFLUXDB_ADMIN_USER из вашего .env
    * Пароль: Значение INFLUXDB_ADMIN_PASSWORD из вашего .env
    * Организация: hydrogen_hub
    * Bucket: telemetry_bucket
* **Grafana**: `http://localhost:3000`
    * Логин/Пароль: значения GRAFANA_ADMIN_USER / GRAFANA_ADMIN_PASSWORD - из вашего .env


##  Структура топиков MQTT и формат данных

Движок слушает MQTT-топик с подстановочными знаками (wildcards)  `hydrogen/telemetry/+`, 
куда датчики шлют JSON-телеметрию:

**Пример сообщения в топик `hydrogen/telemetry/cell_01`:**

```json
{
  "timestamp": 1785590200000,
  "cell_id": "PEMFC-V2-01",
  "h2_pressure_bar": 2.45,
  "air_pressure_bar": 1.82,
  "temperature_celsius": 74.2,
  "current_load_amper": 120.0
}

```

## Что планируется в проекте:
-  Реализация отказоустойчивого буфера на диске при временном отключении InfluxDB (паттерн Circuit Breaker).
-  Подключение модуля Spring AI 2.0 для предиктивного анализа деградации протоннообменной мембраны на основе
накопленной истории.
-  Написание бенчмарков (JMH) для оценки пропускной способности десериализации данных.

