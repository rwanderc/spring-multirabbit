package org.springframework.boot.autoconfigure.amqp;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.util.Assert;

/**
 * PropertiesMultiRabbitConnectionDetailsMap is a structure to hold {@link RabbitConnectionDetails} that are associated
 * to MultiRabbit configuration.
 */
public class PropertiesMultiRabbitConnectionDetailsMap implements MultiRabbitConnectionDetailsMap {

    private final Map<String, RabbitConnectionDetails> map;

    public PropertiesMultiRabbitConnectionDetailsMap(final MultiRabbitProperties multiRabbitProperties) {
        this(toConnDetails(multiRabbitProperties));
    }

    PropertiesMultiRabbitConnectionDetailsMap(final Map<String, RabbitConnectionDetails> connectionDetailsMap) {
        Assert.notNull(connectionDetailsMap, "ConnectionDetails map cannot be null");
        this.map = Collections.unmodifiableMap(connectionDetailsMap);
    }

    private static Map<String, RabbitConnectionDetails> toConnDetails(
            final MultiRabbitProperties multiRabbitProperties) {
        if (multiRabbitProperties == null || multiRabbitProperties.getConnections().isEmpty()) {
            return Collections.emptyMap();
        }
        return multiRabbitProperties.getConnections().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, a -> new PropertiesRabbitConnectionDetails(a.getValue())));
    }

    public Map<String, RabbitConnectionDetails> getMap() {
        return this.map;
    }
}
