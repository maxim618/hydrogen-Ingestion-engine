package com.hydrogen.engine.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate; // обычный шаблон не реактивный
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.ExecutorChannel; //  обработка  -->  в Virtual Threads
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Executors;

@Configuration
public class HydrogenTelemetryReceiver {

    private final String brokerUrl;
    private final String clientId;
    private final String mqttTopic;
    private final HydrogenInfluxRepository influxRepository;   // внедряем репозиторий баз данных

    // инструменты для работы с Valkey/Redis
    private final StringRedisTemplate redisTemplate; // синхронный темплейт для Loom
    private final RedisScript<Long> rateLimitScript;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public HydrogenTelemetryReceiver(
            @Value("${industrial.mqtt.broker-url}") String brokerUrl,//данные из
            @Value("${industrial.mqtt.client-id}") String clientId, //промышленной
            @Value("${industrial.mqtt.topic}") String mqttTopic,   //шины
            HydrogenInfluxRepository influxRepository,
            StringRedisTemplate redisTemplate,
            RedisScript<Long> rateLimitScript) {
        this.brokerUrl = brokerUrl;
        this.clientId = clientId;
        this.mqttTopic = mqttTopic;
        this.influxRepository = influxRepository;
        this.redisTemplate = redisTemplate;
        this.rateLimitScript = rateLimitScript;
    }

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[] { brokerUrl });
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(10);
        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        // выделяем задачу обработки в пул Виртуальных Потоков.
        // каждый пакет будет обрабатываться в своем персональном Virtual Thread.
        return new ExecutorChannel(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Bean
    public MessageProducer inboundAdapter(MqttPahoClientFactory factory, MessageChannel mqttInputChannel) {
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(clientId, factory, mqttTopic);
        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(1);
        adapter.setOutputChannel(mqttInputChannel);
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler handler() {
        // Установим жесткий лимит: максимум 100 сообщений в секунду от одной ячейки
        String maxMessagesPerSecond = "100";

        return new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
                // Извлекаем топик из метаданных MQTT (например, "hydrogen/telemetry/PEMFC-01")
                String receivedTopic = (String) message.getHeaders().get(MqttHeaders.RECEIVED_TOPIC);
                // работает в тысячи раз быстрее, чем выделение памяти под ObjectMapper.readValue().
                // Если скрипт блокирует спам, JSON даже не считывается в память.
                String cellId = extractCellIdFromTopic(receivedTopic);

                // Ключ для счетчика внутри Valkey
                String valkeyKey = "rate:limit:" + cellId;

                try {
                    // используем простой синхронный вызов.
                    // виртуальный поток заблокируется дешево, без накладных расходов Project Reactor.
                    Long rateLimitResult = redisTemplate.execute(
                            rateLimitScript,
                            Collections.singletonList(valkeyKey),
                            maxMessagesPerSecond
                    );

                    if (rateLimitResult != null && rateLimitResult == 0) {
                        System.err.printf("[DROP] Спам отфильтрован! Ячейка %s превысила лимит.%n", cellId);
                        return;
                    }

                    String payload = message.getPayload().toString();
                    TelemetryRecord data = objectMapper.readValue(payload, TelemetryRecord.class);

                    if (data.h2_pressure_bar() > 4.0) {
                        System.err.printf("[ALARM] Критическое давление на %s: %.2f бар!%n", data.cell_id(), data.h2_pressure_bar());
                    } else {
                        influxRepository.save(data); // Быстрая запись в Sinks.Many(библиотека Project Reactor) буфер
                    }

                } catch (IOException e) {
                    System.err.println("[ОШИБКА JSON] Поврежденный пакет телеметрии: " + message.getPayload());
                    // Битый JSON логируем и НЕ прокидываем дальше, чтобы убрать его из очереди брокера
                } catch (Exception e) {
                    System.err.println("[КРИТИЧЕСКИЙ СБОЙ КОНВЕЙЕРА]: " + e.getMessage());
                    //прокидываем инфраструктурные ошибки (например, Valkey упал) вверх.
                    // фреймворк не отправит ACK брокеру, и сообщение перевызовется (Redelivery), предотвращая потерю данных.
                    throw new MessagingException(message, "Conveyor processing failed", e);
                }
            }
        };
    }

    /** быстрый вспомогательный метод извлечения cell_id из MQTT топика
     *  например из строки "hydrogen/telemetry/cell_01" вернет "cell_01"  */
    private String extractCellIdFromTopic(String topic) {
        if (topic == null || !topic.contains("/")) {
            return "unknown_cell";
        }
        return topic.substring(topic.lastIndexOf("/") + 1);
    }
}
