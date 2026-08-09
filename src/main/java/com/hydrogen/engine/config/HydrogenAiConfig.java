package com.hydrogen.engine.config;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;
import java.util.function.Function;

@Configuration
public class HydrogenAiConfig {

    // 1. Регистрируем вашу математическую модель механика как функцию для AI
    @Bean
    @Description("Вычисляет КПД и остаточный ресурс водородной ячейки по текущим датчикам")
    public Function<CellDataRequest, CellDataResponse> calculateCellMetrics() {
        return request -> {
            // Сюда вставляем формулы термодинамики из нашей txt-заметки
            double efficiency = (request.voltage() / 1.48) * 100;
            String status = (efficiency < 50) ? "КРИТИЧЕСКИЙ ИЗНОС" : "НОРМА";
            return new CellDataResponse(efficiency, status);
        };
    }

    // 2. Создаем AI-клиента, который умеет пользоваться этой функцией
    @Bean
    public ChatClient hydrogenAiAssistant(ChatClient.Builder builder) {
        return builder
                .defaultSystem("""
                    Ты — главный инженер-ассистент водородной заправочной станции. 
                    Используй функцию 'calculateCellMetrics' для анализа состояния ячеек.
                    Отвечай профессионально, как инженер-механик.
                    """)
                .build();
    }
}

// Записи (Records) из Java 21 для передачи данных
record CellDataRequest(double voltage, double temperature) {}
record CellDataResponse(double efficiency, String status) {}
