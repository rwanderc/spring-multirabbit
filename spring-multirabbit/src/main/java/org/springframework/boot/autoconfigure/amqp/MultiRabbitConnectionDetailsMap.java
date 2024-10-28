package org.springframework.boot.autoconfigure.amqp;

import java.util.Map;

public interface MultiRabbitConnectionDetailsMap {

    Map<String, RabbitConnectionDetails> getMap();
}
