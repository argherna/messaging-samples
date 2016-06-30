package com.github.argherna.messaging.core;

import java.util.function.Predicate;

public enum MessageProtocol {

  AMQP(5672, 5673), OPENWIRE(61616, 61617), MQTT(1883, 8883);

  private static final Predicate<Void> isRunningWithSsl = Predicates.isRunningWithSsl();

  private final Integer defaultPort;
  
  private final Integer sslPort;
  
  private MessageProtocol(Integer defaultPort, Integer sslPort) {
    this.defaultPort = defaultPort;
    this.sslPort = sslPort;
  }
  
  public Integer getPort() {
    return isRunningWithSsl.test(null) ? sslPort : defaultPort;
  }
}
