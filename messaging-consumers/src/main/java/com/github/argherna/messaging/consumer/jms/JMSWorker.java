package com.github.argherna.messaging.consumer.jms;

import java.util.Properties;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.github.argherna.messaging.consumer.ConsumerService;
import com.github.argherna.messaging.consumer.Worker;
import com.github.argherna.messaging.core.Predicates;

/**
 * Worker implementation run by the {@linkplain ConsumerService} that connects to a message broker
 * on top of the JMS API.
 * 
 * @author andy
 *
 */
public class JMSWorker implements Worker, ExceptionListener {

  private static final Logger LOGGER = Logger.getLogger(JMSWorker.class.getName());

  private Connection connection;
  private MessageConsumer consumer;
  private Session session;
  private final String username;
  private final String password;
  private final ConnectionFactory connectionFactory;
  private final Destination destination;
  private final String clientID;

  private static final Predicate<Void> isRunningWithSsl = Predicates.isRunningWithSsl();

  /**
   * Construct a new JMSWorker.
   * 
   * @param username username to use to connect to the message broker if not using SSL.
   * @param password password to use to connect to the message broker if not using SSL.
   * @param environment the JNDI environment.
   * 
   * @throws NamingException
   */
  public JMSWorker(String username, String password, Properties environment) throws NamingException {
    Context context = new InitialContext(environment);

    // JMS is a standard, but that only goes so far. The ActiveMQ client and Qpid client use
    // different names for their ConnectionFactory implementations.
    if (environment.containsKey("connectionfactory.connectionURI")) {
      connectionFactory = (ConnectionFactory) context.lookup("connectionURI");
    } else {
      connectionFactory = (ConnectionFactory) context.lookup("ConnectionFactory");
    }
    String destname =
        environment.containsKey("queue.consumerQueue") ? "consumerQueue" : "consumerTopic";
    destination = (Destination) context.lookup(destname);
    this.username = username;
    this.password = password;
    
    // Clients have to have a unique ID.
    this.clientID =
        String.format("consumers/JMSWorker+%s-%d", environment.getProperty("protocol"),
            System.currentTimeMillis());
  }

  @Override
  public void startUp() throws Exception {

    if (isRunningWithSsl.test(null)) {
      connection = connectionFactory.createConnection();
    } else {
      connection = connectionFactory.createConnection(username, password);
    }
    connection.setExceptionListener(this);
    connection.setClientID(clientID);
    connection.start();

    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    consumer = session.createConsumer(destination);
  }

  @Override
  public void run() {
    try {
      // Blocks until a message is received or the consumer is closed.
      Message m = consumer.receive();
      if (m instanceof TextMessage) {
        TextMessage textMessage = (TextMessage) m;
        String text = textMessage.getText();
        LOGGER.info(String.format("Received: %s", text));
      } else {
        LOGGER.info(String.format("Received: %s", m));
      }
    } catch (JMSException e) {
      LOGGER.warning(e.getMessage());
    }
  }

  @Override
  public void triggerShutdown() {
    try {
      // Unblocks the run method
      consumer.close();
    } catch (JMSException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void shutDown() throws Exception {
    LOGGER.info("Closing broker connection");
    session.close();
    connection.close();
  }

  @Override
  public void onException(JMSException ex) {
    LOGGER.log(Level.WARNING, "Messaging exception", ex);
  }
}
