package com.hydrogen.engine.config;
import com.hydrogen.engine.dto.CellDataRequest;
import com.hydrogen.engine.dto.CellDataResponse;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;
import java.util.function.Function;

@Configuration
public class HydrogenAiConfig {

    // регистрируем математическую модель механика как функцию для AI
    @Bean
    @Description("Вычисляет КПД и остаточный ресурс водородной ячейки по текущим датчикам")
    public Function<CellDataRequest, CellDataResponse> calculateCellMetrics() {
        return new Function<CellDataRequest, CellDataResponse>() {
            @Override
            public CellDataResponse apply(CellDataRequest request) {
                // здесь формулы термодинамики
                double efficiency = (request.voltage() / 1.48) * 100;
                String status = (efficiency < 50) ? "КРИТИЧЕСКИЙ ИЗНОС" : "НОРМА";
                return new CellDataResponse(efficiency, status);
            }
        };
    }

    // AI-клиент, который будет пользоваться этой функцией
    @Bean
    public ChatClient hydrogenAiAssistant(ChatClient.Builder builder) {
        return builder
                .defaultSystem("""
                    Ты - главный инженер-ассистент водородной заправочной станции. 
                    Используй функцию 'calculateCellMetrics' для анализа состояния ячеек.
                    Отвечай профессионально, как инженер-механик.
                    """)
                .build();
    }

}
