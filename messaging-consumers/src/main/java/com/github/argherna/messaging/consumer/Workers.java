package com.github.argherna.messaging.consumer;

import static com.github.argherna.messaging.core.MessageProtocol.AMQP;
import static com.github.argherna.messaging.core.MessageProtocol.MQTT;
import static com.github.argherna.messaging.core.MessageProtocol.OPENWIRE;

import java.util.Properties;
import java.util.function.Predicate;

import javax.naming.Context;

import com.github.argherna.messaging.consumer.jms.JMSWorker;
import com.github.argherna.messaging.consumer.mqtt.MQTTWorker;
import com.github.argherna.messaging.core.MessageProtocol;
import com.github.argherna.messaging.core.Predicates;

/**
 * Worker factories.
 * 
 * @author andy
 *
 */
public class Workers {

  private Workers() {}

  private static final Predicate<String> isPopulated = (Predicates.isNull()
      .or(Predicates.isEmpty())).negate();

  private static final Predicate<Void> isRunningWithSsl = Predicates.isRunningWithSsl();

  /**
   * Returns a Worker.
   * 
   * @param protocol the MessageProtocol to base construction of the Worker.
   * @param username username to use to connect to the message broker if not using SSL.
   * @param password password to use to connect to the message broker if not using SSL.
   * @param hostname hostname of the message broker.
   * @param port port the message broker server.
   * @param queuename name of a queue to send messages to if no topicname is set.
   * @param topicname name of a topic to send messages to if no queuename is set.
   * @return a Worker.
   * 
   * @throws Exception
   */
  public static Worker getWorker(MessageProtocol protocol, String username, String password,
      String hostname, Integer port, String queuename, String topicname) throws Exception {

    Worker worker = null;

    Properties environment = new Properties();
    if (isPopulated.test(queuename)) {
      environment.setProperty("queue.consumerQueue", queuename);
    } else if (isPopulated.test(topicname)) {
      environment.setProperty("topic.consumerTopic", topicname);
    } else {
      throw new RuntimeException("Neither queuename nor topicname set!");
    }
    environment.setProperty("protocol", protocol.name());

    if (protocol == OPENWIRE) {

      environment.setProperty(Context.INITIAL_CONTEXT_FACTORY,
          "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
      String uriTemplate = isRunningWithSsl.test(null) ? "ssl://%s:%d" : "tcp://%s:%d";
      environment.setProperty(Context.PROVIDER_URL, String.format(uriTemplate, hostname, port));
      worker = new JMSWorker(username, password, environment);

    } else if (protocol == AMQP) {

      environment.setProperty(Context.INITIAL_CONTEXT_FACTORY,
          "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
      String uriTemplate = isRunningWithSsl.test(null) ? "amqps://%s:%d" : "amqp://%s:%d";
      environment.setProperty("connectionfactory.connectionURI",
          String.format(uriTemplate, hostname, port));
      worker = new JMSWorker(username, password, environment);

    } else if (protocol == MQTT) {

      worker = new MQTTWorker(username, password, hostname, port, topicname);

    }
    return worker;

  }
}
