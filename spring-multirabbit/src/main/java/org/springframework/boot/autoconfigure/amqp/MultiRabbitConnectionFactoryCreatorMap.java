package org.springframework.boot.autoconfigure.amqp;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration.RabbitConnectionFactoryCreator;
import org.springframework.util.Assert;

/**
 * MultiRabbitConnectionFactoryCreatorMap holds a map of {@link RabbitConnectionFactoryCreator} that are
 * associated to MultiRabbit configuration.
 */
public class MultiRabbitConnectionFactoryCreatorMap {

    private final Map<String, RabbitConnectionFactoryCreator> map;

    MultiRabbitConnectionFactoryCreatorMap(final MultiRabbitProperties multiRabbitProperties) {
        this(toFactoryCreator(multiRabbitProperties));
    }

    MultiRabbitConnectionFactoryCreatorMap(final Map<String, RabbitConnectionFactoryCreator> map) {
        Assert.notNull(map, "Map cannot be null");
        this.map = Collections.unmodifiableMap(map);
    }

    private static Map<String, RabbitConnectionFactoryCreator> toFactoryCreator(
            final MultiRabbitProperties multiRabbitProperties) {
        if (multiRabbitProperties == null || multiRabbitProperties.getConnections().isEmpty()) {
            return Collections.emptyMap();
        }
        return multiRabbitProperties.getConnections().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> new RabbitConnectionFactoryCreator(e.getValue())));
    }

    Map<String, RabbitConnectionFactoryCreator> getMap() {
        return this.map;
    }
}
