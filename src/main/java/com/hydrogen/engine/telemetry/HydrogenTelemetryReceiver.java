package com.hydrogen.engine.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import java.io.IOException;

@Configuration
public class HydrogenTelemetryReceiver {

    private final String brokerUrl;
    private final String clientId;
    private final String mqttTopic;
    private final HydrogenInfluxRepository influxRepository; // Внедряем наш репозиторий баз данных

    // Чистый конструктор без единого @Autowired - стандарт современной Java
    public HydrogenTelemetryReceiver(
            @Value("${industrial.mqtt.broker-url}") String brokerUrl,
            @Value("${industrial.mqtt.client-id}") String clientId,
            @Value("${industrial.mqtt.topic}") String mqttTopic,
            HydrogenInfluxRepository influxRepository) {
        this.brokerUrl = brokerUrl;
        this.clientId = clientId;
        this.mqttTopic = mqttTopic;
        this.influxRepository = influxRepository;
    }

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[] { brokerUrl });
        options.setAutomaticReconnect(true); // Защита от обрыва кабеля сети
        options.setConnectionTimeout(10);
        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageProducer inboundAdapter(MqttPahoClientFactory factory, MessageChannel mqttInputChannel) {
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(clientId, factory, mqttTopic);
        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setOutputChannel(mqttInputChannel);
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler handler() {
        ObjectMapper objectMapper = new ObjectMapper();

        return message -> {
            String payload = message.getPayload().toString();
            try {
                // Парсим JSON в Record
                TelemetryRecord data = objectMapper.readValue(payload, TelemetryRecord.class);

                // Проверка механика (Fast-Fail)
                if (data.h2_pressure_bar() > 4.0) {
                    System.err.printf("⚠️ [АЛАРМ] Критическое давление водорода на ячейке %s: %.2f бар!%n",
                            data.cell_id(), data.h2_pressure_bar());
                } else {
                    // Передаем данные в реактивный репозиторий InfluxDB
                    influxRepository.save(data);
                }

            } catch (IOException e) {
                System.err.println("[ОШИБКА JSON] Поврежденный пакет телеметрии: " + payload);
            }
        };
    }
}
