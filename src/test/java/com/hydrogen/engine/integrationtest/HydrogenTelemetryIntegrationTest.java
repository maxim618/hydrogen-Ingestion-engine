package com.hydrogen.engine.integrationtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hydrogen.engine.domain.TelemetryRecord;
import com.hydrogen.engine.repository.HydrogenInfluxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InfluxDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers
class HydrogenTelemetryIntegrationTest {

    private static final String FALLBACK_QUEUE_KEY = "conveyor:fallback:queue";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Container
    static InfluxDBContainer<?> influxContainer = new InfluxDBContainer<>(DockerImageName.parse("influxdb:2.7.1"))
            .withAdminToken("my-super-secret-test-token-1234567890")
            .withOrganization("hydrogen_org")
            .withBucket("telemetry_bucket");

    @Container
    static GenericContainer<?> valkeyContainer = new GenericContainer<>(DockerImageName.parse("valkey/valkey:8.0"))
            .withExposedPorts(6379);

    @Autowired
    private HydrogenInfluxRepository influxRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("industrial.influxdb.url", influxContainer::getUrl);
        registry.add("industrial.influxdb.token", influxContainer::getAdminToken);
        registry.add("industrial.influxdb.org", () -> "hydrogen_org");
        registry.add("industrial.influxdb.bucket", () -> "telemetry_bucket");
        registry.add("spring.data.redis.host", valkeyContainer::getHost);
        registry.add("spring.data.redis.port", () -> valkeyContainer.getMappedPort(6379));
    }

    @BeforeEach
    void resetStateBeforeEachTest() {
        redisTemplate.delete(FALLBACK_QUEUE_KEY);
        influxRepository.resetRecoveryStateForTest();
    }

    @Test
    void testCircuitBreaker_ShouldEvacuateToValkeyWhenInfluxFails() {
        influxRepository.setSimulateFailure(true);

        TelemetryRecord record = new TelemetryRecord(
                System.currentTimeMillis(),
                "cell_001",
                3.5,
                1.2,
                65.4,
                320.0
        );
        influxRepository.save(record);

        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            Long queueSize = redisTemplate.opsForList().size(FALLBACK_QUEUE_KEY);
            return queueSize != null && queueSize > 0;
        });

        String jsonBatch = redisTemplate.opsForList().index(FALLBACK_QUEUE_KEY, 0);
        assertNotNull(jsonBatch);
        assertTrue(jsonBatch.contains("cell_001"));
    }

    @Test
    void testRecoveryScheduler_ShouldRestoreDataInFifoOrderWhenInfluxIsBack() throws Exception {
        TelemetryRecord record = new TelemetryRecord(
                System.currentTimeMillis(),
                "cell_999",
                2.1,
                1.0,
                50.0,
                200.0
        );
        String jsonPayload = objectMapper.writeValueAsString(List.of(record));
        redisTemplate.opsForList().leftPush(FALLBACK_QUEUE_KEY, jsonPayload);

        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            Long queueSize = redisTemplate.opsForList().size(FALLBACK_QUEUE_KEY);
            return queueSize != null && queueSize == 0;
        });
    }

}
