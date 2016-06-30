package com.github.argherna.messaging.consumer.mqtt;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.github.argherna.messaging.consumer.ConsumerService;
import com.github.argherna.messaging.consumer.Worker;
import com.github.argherna.messaging.core.Predicates;

/**
 * Worker implementation run by the {@linkplain ConsumerService} that connects to a message broker
 * on top of the MQTT protocol.
 * 
 * @author andy
 *
 */
public class MQTTWorker implements Worker, MqttCallback {

  private static final Logger LOGGER = Logger.getLogger(MQTTWorker.class.getName());

  private CountDownLatch done;

  private final MqttConnectOptions connectionOptions;

  private final IMqttClient client;

  /**
   * Constructs a new MQTTWorker.
   * 
   * @param username username to use to connect to the message broker if not using SSL.
   * @param password password to use to connect to the message broker if not using SSL.
   * @param hostname hostname of the message broker.
   * @param port port the message broker server.
   * @param topicname name of a topic to send messages to if no queuename is set.
   * @throws MqttException
   */
  public MQTTWorker(String username, String password, String hostname, Integer port,
      String topicname) throws MqttException {

    connectionOptions = new MqttConnectOptions();
    String uri = null;
    if (Predicates.isRunningWithSsl().test(null)) {
      uri = String.format("ssl://%s:%d", hostname, port);
    } else {
      uri = String.format("tcp://%s:%d", hostname, port);
      connectionOptions.setUserName(username);
      connectionOptions.setPassword(password.toCharArray());
    }
    client = new MqttClient(uri, "consumers/MQTTWorker");
    client.connect(connectionOptions);

    // This is where the work will be done.
    client.setCallback(this);
    client.subscribe(topicname);
  }

  @Override
  public void startUp() throws Exception {
    done = new CountDownLatch(1);
  }

  @Override
  public void run() {
    try {
      // Block until a shutdown is requested.
      done.await();
    } catch (InterruptedException e) {
      LOGGER.warning("Interrupted while running!");
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void triggerShutdown() {
    done.countDown();
  }

  @Override
  public void shutDown() throws Exception {
    client.disconnect();
  }

  /**
   * {@inheritDoc}
   * 
   * <p>
   * Logs a message at {@linkplain Level#SEVERE SEVERE} level that the connection to the server was
   * lost.
   * </p>
   */
  @Override
  public void connectionLost(Throwable cause) {
    LOGGER.log(Level.SEVERE, "Lost connection to server", cause);
  }

  /**
   * {@inheritDoc}
   * 
   * <p>
   * Logs a message at {@linkplain Level#INFO INFO} level that a message was received. This is the
   * main work method for the MQTT consumer client.
   * </p>
   */
  @Override
  public void messageArrived(String topic, MqttMessage message) throws Exception {
    LOGGER.info(String.format("Received: %s", new String(message.getPayload())));
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {}
}
