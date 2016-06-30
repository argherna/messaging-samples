package com.github.argherna.messaging.producer;

import static com.github.argherna.messaging.core.MessageProtocol.AMQP;
import static com.github.argherna.messaging.core.MessageProtocol.OPENWIRE;
import static com.github.argherna.messaging.core.Predicates.isJmsSupported;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.github.argherna.messaging.core.MessageProtocol;
import com.github.argherna.messaging.core.Predicates;

/**
 * Produces a message to send to the message broker.
 */
public class JMSProducer implements Runnable {

  private static final Logger LOGGER = Logger.getLogger(JMSProducer.class.getName());

  public static final String DEFAULT_HOSTNAME = "localhost";

  public static final String DEFAULT_PASSWORD = "password";

  public static final String DEFAULT_USERNAME = "admin";

  public static final MessageProtocol DEFAULT_PROTOCOL = OPENWIRE;

  private final Collection<String> filesToSend;

  private final String password;

  private final String username;

  private final Destination destination;

  private final ConnectionFactory connectionFactory;

  private static final Predicate<Void> isRunningWithSsl = Predicates.isRunningWithSsl();

  private static final Predicate<String> isPopulated = (Predicates.isNull()
      .or(Predicates.isEmpty())).negate();

  /**
   * Constructs a new JMSProducer.
   * 
   * @param username username to use to connect to the message broker if not using SSL.
   * @param password password to use to connect to the message broker if not using SSL.
   * @param hostname hostname of the message broker.
   * @param port port the message broker server.
   * @param queuename name of a queue to send messages to if no topicname is set.
   * @param topicname name of a topic to send messages to if no queuename is set.
   * @param filesToSend a Collection of filenames to send to the broker.
   * @param environment the JNDI environment.
   * @throws NamingException
   */
  public JMSProducer(String username, String password, String hostname, Integer port,
      String queuename, String topicname, Collection<String> filesToSend, Properties environment)
      throws NamingException {
    this.username = username;
    this.password = password;
    this.filesToSend = filesToSend;

    Context context = new InitialContext(environment);
    if (environment.containsKey("connectionfactory.connectionURI")) {
      connectionFactory = (ConnectionFactory) context.lookup("connectionURI");
    } else {
      connectionFactory = (ConnectionFactory) context.lookup("ConnectionFactory");
    }
    String destname =
        environment.containsKey("queue.consumerQueue") ? "consumerQueue" : "consumerTopic";
    destination = (Destination) context.lookup(destname);
  }

  @Override
  public void run() {

    Connection connection = null;
    try {
      if (isRunningWithSsl.test(null)) {
        connection = connectionFactory.createConnection();
      } else {
        connection = connectionFactory.createConnection(username, password);
      }

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(destination);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      for (String fileToSend : filesToSend) {
        LOGGER.finer(String.format("Preparing to send %s ...", fileToSend));
        try {
          byte[] data = Files.readAllBytes(Paths.get(fileToSend));
          String text = new String(data);
          LOGGER.finer(String.format("Sending message: %s", text));
          TextMessage message = session.createTextMessage(text);
          producer.send(message);
          LOGGER.finer("Message sent OK");
        } catch (IOException e) {
          LOGGER.warning(String.format("Problem reading file %s: %s", fileToSend, e.getMessage()));
        }
      }
      session.close();
      connection.close();
    } catch (JMSException e) {
      e.printStackTrace();
    }
  }

  /**
   * Main for running this program.
   * 
   * <h3>Options and Arguments</h3>
   * 
   * <p>
   * There are several command-line options and arguments. They are:
   * </p>
   * <table>
   * <thead>
   * <tr>
   * <th>Option</th>
   * <th>Description</th>
   * <th>Default</th>
   * </tr>
   * </thead> <tbody>
   * <tr>
   * <td>{@code -help}</td>
   * <td>Prints a help message and exits.</td>
   * <td>N/A</td>
   * </tr>
   * <tr>
   * <td>{@code -host <hostname>}</td>
   * <td>Name of message broker host</td>
   * <td>localhost</td>
   * </tr>
   * <tr>
   * <td>{@code -password <arg>}</td>
   * <td>Message broker connection password</td>
   * <td>password</td>
   * </tr>
   * <tr>
   * <td>{@code -port <port>}</td>
   * <td>Port message broker is listening on</td>
   * <td>61616</td>
   * </tr>
   * <tr>
   * <td>{@code -queuename <name>}</td>
   * <td>Queue to subscribe to</td>
   * <td>N/A</td>
   * </tr>
   * <tr>
   * <td>{@code -topicname <name>}</td>
   * <td>Topic to subscribe to</td>
   * <td>N/A</td>
   * </tr>
   * <tr>
   * <td>{@code -username <username>}</td>
   * <td>Message broker connection user name</td>
   * <td>admin</td>
   * </tr>
   * </tbody>
   * </table>
   * 
   * <p>
   * The arguments for this program are 1 or more file names. The files must be specified by their
   * absolute path, must exist, and must be readable by the user executing this program.
   * </p>
   * 
   * <h3>Security</h3>
   * 
   * <p>
   * Depending on how this program is run will determine how it will connect to the message broker.
   * By default, it will connect using the default username and password. These defaults are
   * overridden on the command line.
   * </p>
   * 
   * <p>
   * If the {@code javax.net.ssl} system properties are specified, a connection is made over SSL and
   * the username and password are completely ignored.
   * </p>
   * 
   * <h3>Requirements</h3>
   * 
   * <p>
   * The only required option is one of either {@code -queuename} or {@code -topicname}. The program
   * will not run if neither is or both are specified.
   * </p>
   * 
   * <p>
   * At least 1 filename has to be specified. If none are specifed, the program exits with a code of
   * 1.
   * </p>
   *
   * <h3>Exit Codes</h3>
   * 
   * <dl>
   * <dt>0</dt>
   * <dd>Program ran successfully without errors.</dd>
   * <dt>1</dt>
   * <dd>Program encountered 1 or more errors including but not limited to missing values for
   * specified options, no file names specified, or an exception was encountered during execution.</dd>
   * <dt>2</dt>
   * <dd>Program was run with the {@code -help} option specified.</dd>
   * <dl>
   * 
   * @param args the command line arguments
   */
  public static void main(String... args) {

    List<String> filesToSend = new ArrayList<>();
    String hostname = DEFAULT_HOSTNAME;
    String password = DEFAULT_PASSWORD;
    Integer port = null;
    MessageProtocol protocol = DEFAULT_PROTOCOL;
    String queuename = null;
    String topicname = null;
    String username = DEFAULT_USERNAME;

    BiPredicate<String[], Integer> noElementAfter = Predicates.hasElementAfter().negate();

    int argIdx = 0;
    while (argIdx < args.length) {
      String arg = args[argIdx];
      switch (arg) {
        case "-help":
          showUsageAndExit(2);
          break;

        case "-host":
          if (noElementAfter.test(args, argIdx)) {
            System.err.println("-host not set!");
            System.err.println("");
            showUsageAndExit(1);
          } else {
            hostname = args[++argIdx];
          }
          break;

        case "-password":
          if (noElementAfter.test(args, argIdx)) {
            System.err.println("-password not set!");
            System.err.println("");
            showUsageAndExit(1);
          } else {
            password = args[++argIdx];
          }
          break;

        case "-port":
          if (noElementAfter.test(args, argIdx)) {
            System.err.println("-port not set!");
            System.err.println("");
            showUsageAndExit(1);
          } else {
            String portarg = args[++argIdx];
            try {
              port = Integer.valueOf(portarg);
            } catch (NumberFormatException e) {
              System.err.printf("port value %s has to be numeric!%n", portarg);
              System.exit(1);
            }
          }
          break;

        case "-protocol":
          if (noElementAfter.test(args, argIdx)) {
            System.err.println("-protocol not set!");
            System.err.println("");
            showUsageAndExit(1);
          } else {
            String protocolarg = args[++argIdx];
            try {
              protocol = MessageProtocol.valueOf(protocolarg.toUpperCase());
            } catch (Exception e) {
              System.err.printf("Unsupported protocol %s%n", protocolarg);
              System.exit(1);
            }
          }
          break;

        case "-queuename":
          if (noElementAfter.test(args, argIdx)) {
            System.err.println("-queuename not set!");
            System.err.println("");
            showUsageAndExit(1);
          } else {
            queuename = args[++argIdx];
          }
          break;

        case "-topicname":
          if (noElementAfter.test(args, argIdx)) {
            System.err.println("-topicname not set!");
            System.err.println("");
            showUsageAndExit(1);
          } else {
            topicname = args[++argIdx];
          }
          break;

        case "-username":
          if (noElementAfter.test(args, argIdx)) {
            System.err.println("-protocol not set!");
            System.err.println("");
            showUsageAndExit(1);
          } else {
            username = args[++argIdx];
          }
          break;

        default:
          filesToSend.add(arg);
          break;
      }

      argIdx++;
    }

    BiPredicate<String, String> areNullOrEmpty = Predicates.areNull().or(Predicates.areEmpty());
    BiPredicate<String, String> arePopulated = Predicates.arePopulated();
    if (areNullOrEmpty.test(queuename, topicname)) {
      System.err.println("Must set one of either -queuename or -topicname");
      System.err.println("");
      showUsageAndExit(1);
    } else if (arePopulated.test(queuename, topicname)) {
      System.err.println("Must set only one of either -queuename or -topicname");
      System.err.println("");
      showUsageAndExit(1);
    }

    if ((isJmsSupported()).negate().test(protocol)) {
      System.err.printf("Unsupported protocol %s%n", protocol);
      System.exit(1);
    }

    if (port == null) {
      port = protocol.getPort();
    }

    try {
      Properties environment = new Properties();
      if (isPopulated.test(queuename)) {
        environment.setProperty("queue.consumerQueue", queuename);
      } else if (isPopulated.test(topicname)) {
        environment.setProperty("topic.consumerTopic", topicname);
      } else {
        throw new RuntimeException("Neither queuename nor topicname set!");
      }
      environment.setProperty("protocol", protocol.name());

      if (protocol == AMQP) {

        environment.setProperty(Context.INITIAL_CONTEXT_FACTORY,
            "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        String uriTemplate = isRunningWithSsl.test(null) ? "amqps://%s:%d" : "amqp://%s:%d";
        environment.setProperty("connectionfactory.connectionURI",
            String.format(uriTemplate, hostname, port));

      } else {

        environment.setProperty(Context.INITIAL_CONTEXT_FACTORY,
            "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        String uriTemplate = isRunningWithSsl.test(null) ? "ssl://%s:%d" : "tcp://%s:%d";
        environment.setProperty(Context.PROVIDER_URL, String.format(uriTemplate, hostname, port));

      }

      LOGGER.finest(environment.toString());
      JMSProducer producer =
          new JMSProducer(username, password, hostname, port, queuename, topicname, filesToSend,
              environment);
      producer.run();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      if (LOGGER.isLoggable(Level.FINEST)) {
        e.printStackTrace();
      }
      System.exit(1);
    }

  }

  private static void showUsageAndExit(int code) {
    showUsage();
    System.exit(code);
  }

  private static void showUsage() {
    System.err.printf("%s -queuename <name> | -topicname <name> [OPTION] FILE [FILE FILE]...%n",
        JMSProducer.class.getName());
    System.err.println("");
    System.err.println("Sends the named file(s) to the specified queue or topic.");
    System.err.println("");
    System.err.println("Options:");
    System.err.println("");
    System.err.println(" -help                 show this message and exit");
    System.err.println(" -host <hostname>      connect to the host running the message broker");
    System.err.println("                         (default is localhost)");
    System.err
        .println(" -password <arg>       message broker connection password (default is password)");
    System.err
        .println(" -port <port>          port message broker is running on (default is 61616)");
    System.err.println(" -protocol <protocol>  message protocol to use (default is openwire)");
    System.err.println(" -queuename <name>     the queue to subscribe to");
    System.err.println(" -topicname <name>     the topic to subscribe to");
    System.err
        .println(" -username <username>  message broker connection username (default is admin)");
    System.err.println("");
    System.err.println("Note:");
    System.err.println("");
    System.err.println("The -username and -password options are ignored when using SSL.");
    System.err
        .println("A -queuename or -topicname must be set; it is an error to set both or neither.");
  }
}
