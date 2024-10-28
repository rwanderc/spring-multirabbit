package org.springframework.boot.autoconfigure.amqp;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PropertiesMultiRabbitConnectionDetailsMapTest {

    @Test
    void shouldCreateMapFromMultiRabbitProperties() {
        final String dummyKey = "dummy-key";
        final String address = "dummy-address";
        final int port = 1234;

        final RabbitProperties rabbitProperties = new RabbitProperties();
        rabbitProperties.setUsername("dummy-username");
        rabbitProperties.setPassword("dummy-password");
        rabbitProperties.setVirtualHost("dummy-virtual-host");
        rabbitProperties.setAddresses("amqps://%s:%d".formatted(address, port));
        final MultiRabbitProperties multiRabbitProperties = new MultiRabbitProperties();
        multiRabbitProperties.getConnections().put(dummyKey, rabbitProperties);

        final PropertiesMultiRabbitConnectionDetailsMap actual
                = new PropertiesMultiRabbitConnectionDetailsMap(multiRabbitProperties);

        assertThat(actual.getMap()).hasSize(1);
        final RabbitConnectionDetails connectionDetails = actual.getMap().get(dummyKey);

        assertThat(connectionDetails.getUsername()).isEqualTo(rabbitProperties.getUsername());
        assertThat(connectionDetails.getPassword()).isEqualTo(rabbitProperties.getPassword());
        assertThat(connectionDetails.getVirtualHost()).isEqualTo(rabbitProperties.getVirtualHost());
        assertThat(connectionDetails.getFirstAddress().host()).isEqualTo(address);
        assertThat(connectionDetails.getFirstAddress().port()).isEqualTo(port);
    }
}
