package org.springframework.boot.autoconfigure.amqp;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.assertj.core.data.TemporalUnitLessThanOffset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.stereotype.Component;
import org.testcontainers.containers.RabbitMQContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class MultiRabbitRetryTest {

    private final RabbitMQContainer broker0 = new RabbitMQContainer("rabbitmq:3-management");
    private final RabbitMQContainer broker1 = new RabbitMQContainer("rabbitmq:3-management");

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner().withConfiguration(
            AutoConfigurations.of(MultiRabbitAutoConfiguration.class, RabbitAutoConfiguration.class));

    @BeforeEach
    void beforeEach() {
        this.broker0.start();
        this.broker1.start();
    }

    @AfterEach
    void afterEach() {
        this.broker0.stop();
        this.broker1.stop();
    }

    @Test()
    @DisplayName("should ensure retry logic for multirabbit listeners")
    void shouldEnsureRetryLogicForMultiRabbitConnections() {
        final Integer broker0Port = broker0.getMappedPort(5672);
        final long broker0RetryInterval = 10;
        final int broker0Attempts = 3;
        final Integer broker1Port = broker1.getMappedPort(5672);
        final long broker1RetryInterval = 50;
        final int broker1Attempts = 5;

        final List<String> properties = Arrays.asList(
                "spring.rabbitmq.port=" + broker0Port,
                "spring.rabbitmq.listener.simple.retry.enabled=true",
                String.format("spring.rabbitmq.listener.simple.retry.initial-interval=%d", broker0RetryInterval),
                String.format("spring.rabbitmq.listener.simple.retry.max-attempts=%d", broker0Attempts),
                "spring.multirabbitmq.enabled=true",
                String.format("spring.multirabbitmq.connections.%s.port=%d", TestListeners.BROKER_NAME_1, broker1Port),
                String.format("spring.multirabbitmq.connections.%s.listener.simple.retry.enabled=true",
                        TestListeners.BROKER_NAME_1),
                String.format("spring.multirabbitmq.connections.%s.listener.simple.retry.initial-interval=%d",
                        TestListeners.BROKER_NAME_1, broker1RetryInterval),
                String.format("spring.multirabbitmq.connections.%s.listener.simple.retry.max-attempts=%d",
                        TestListeners.BROKER_NAME_1, broker1Attempts));

        this.contextRunner.withPropertyValues(properties.toArray(new String[0]))
                .withBean(TestListeners.class)
                .run((context) -> {
                    final RabbitTemplate rabbitTemplate = context.getBean(RabbitTemplate.class);
                    rabbitTemplate.convertAndSend(TestListeners.EXCHANGE_0, TestListeners.ROUTING_KEY_0,
                            "test-broker0");
                    SimpleResourceHolder.bind(rabbitTemplate.getConnectionFactory(), TestListeners.BROKER_NAME_1);
                    try {
                        rabbitTemplate.convertAndSend(TestListeners.EXCHANGE_1, TestListeners.ROUTING_KEY_1,
                                "test-broker1");
                    } finally {
                        SimpleResourceHolder.unbind(rabbitTemplate.getConnectionFactory());
                    }

                    Thread.sleep(Math.max(broker0RetryInterval, broker1RetryInterval));
                    assertListenerRetryLogic(broker0RetryInterval, broker0Attempts,
                            TestListeners.BROKER_0_RETRY_INSTANTS);
                    assertListenerRetryLogic(broker1RetryInterval, broker1Attempts,
                            TestListeners.BROKER_1_RETRY_INSTANTS);
                });
    }

    private void assertListenerRetryLogic(final long retryInterval,
                                          final int attempts,
                                          final List<Instant> instants) {
        await().pollInterval(Duration.ofMillis(10))
                .atMost(Duration.ofMillis(2 * retryInterval * attempts))
                .until(() -> instants.size() == attempts);
        assertThat(instants).hasSize(attempts);
        for (int i = 0; i < instants.size() - 1; i++) {
            assertThat(instants.get(i)).isCloseTo(instants.get(i + 1),
                    new TemporalUnitLessThanOffset(2 * retryInterval, ChronoUnit.MILLIS));
        }
    }

    @Component
    @EnableRabbit
    private static class TestListeners {

        public static final String EXCHANGE_0 = "exchange0";
        public static final String ROUTING_KEY_0 = "routingKey0";
        public static final String QUEUE_0 = "queue0";

        public static final String BROKER_NAME_1 = "broker1";
        public static final String EXCHANGE_1 = "exchange1";
        public static final String ROUTING_KEY_1 = "routingKey1";
        public static final String QUEUE_1 = "queue1";

        public static final List<Instant> BROKER_0_RETRY_INSTANTS = new ArrayList<>();
        public static final List<Instant> BROKER_1_RETRY_INSTANTS = new ArrayList<>();

        @RabbitListener(bindings = @QueueBinding(
                exchange = @Exchange(EXCHANGE_0),
                value = @Queue(QUEUE_0),
                key = ROUTING_KEY_0))
        void listenBroker0(final String message) {
            BROKER_0_RETRY_INSTANTS.add(Instant.now());
            throw new RuntimeException("dummy-exception");
        }

        @RabbitListener(containerFactory = BROKER_NAME_1, bindings = @QueueBinding(
                exchange = @Exchange(EXCHANGE_1),
                value = @Queue(QUEUE_1),
                key = ROUTING_KEY_1))
        void listenBroker1(final String message) {
            BROKER_1_RETRY_INSTANTS.add(Instant.now());
            throw new RuntimeException("dummy-exception");
        }
    }
}
