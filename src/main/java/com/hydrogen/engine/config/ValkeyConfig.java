package com.hydrogen.engine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

@Configuration
public class ValkeyConfig {

    @Bean
    public RedisScript<Long> rateLimitScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        // Указываем путь к нашему Lua-файлу
        script.setLocation(new ClassPathResource("scripts/rate_limiter.lua"));
        // Скрипт возвращает число (0 или 1), в Java это будет Long
        script.setResultType(Long.class);
        return script;
    }
}
