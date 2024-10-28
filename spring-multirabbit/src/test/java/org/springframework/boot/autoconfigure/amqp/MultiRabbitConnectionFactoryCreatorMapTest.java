package org.springframework.boot.autoconfigure.amqp;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration.RabbitConnectionFactoryCreator;
import static org.assertj.core.api.Assertions.assertThat;

class MultiRabbitConnectionFactoryCreatorMapTest {

    @Test
    void shouldCreateWithMultiRabbitProperties() {
        final String dummyKey = "dummy-key";
        final MultiRabbitProperties multiRabbitProperties = new MultiRabbitProperties();
        multiRabbitProperties.getConnections().put(dummyKey, new RabbitProperties());

        final MultiRabbitConnectionFactoryCreatorMap multiRabbitConnectionFactoryCreatorMap
                = new MultiRabbitConnectionFactoryCreatorMap(multiRabbitProperties);

        assertThat(multiRabbitConnectionFactoryCreatorMap.getMap()).hasSize(1);
        assertThat(multiRabbitConnectionFactoryCreatorMap.getMap().get(dummyKey))
                .isInstanceOf(RabbitConnectionFactoryCreator.class);
    }

    @Test
    void shouldCreateWithRabbitConnectionFactoryCreatorMap() {
        final String dummyKey = "dummy-key";
        final RabbitProperties rabbitProperties = new RabbitProperties();

        final Map<String, RabbitConnectionFactoryCreator> creators
                = Map.of(dummyKey, new RabbitConnectionFactoryCreator(rabbitProperties));

        final MultiRabbitConnectionFactoryCreatorMap multiRabbitConnectionFactoryCreatorMap
                = new MultiRabbitConnectionFactoryCreatorMap(creators);

        assertThat(multiRabbitConnectionFactoryCreatorMap.getMap()).hasSize(1);
        assertThat(multiRabbitConnectionFactoryCreatorMap.getMap().get(dummyKey))
                .isInstanceOf(RabbitConnectionFactoryCreator.class);
    }
}
