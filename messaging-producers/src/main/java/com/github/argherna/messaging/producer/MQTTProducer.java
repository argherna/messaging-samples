package com.github.argherna.messaging.producer;

import static com.github.argherna.messaging.core.MessageProtocol.MQTT;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.github.argherna.messaging.core.Predicates;

/**
 * Produces 1 or more messages to send to the message broker over the MQTT protocol.
 * 
 * <p>
 * The message content comes from file name specified on the command line. The MQTT protocol sends
 * messages as raw bytes. JMS consumers must be able to convert the raw bytes into something they
 * can work with.
 * </p>
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html">MQTT Protocol
 *      Specification</a>
 * @see <a
 *      href="http://www.ibm.com/support/knowledgecenter/api/content/nl/en-us/SSCGGQ_1.2.0/com.ibm.ism.doc/Planning/pl00005.html">Conversion
 *      between MQTT and JMS</a>
 * @see <a
 *      href="http://stackoverflow.com/questions/30723073/how-to-produce-from-mqtt-and-consume-as-mqtt-and-jms-in-activemq">Consuming
 *      MQTT as JMS in ActiveMQ</a>
 */
public class MQTTProducer implements Runnable {

  private static final Logger LOGGER = Logger.getLogger(MQTTProducer.class.getName());

  /**
   * The default host name ({@value #DEFAULT_HOSTNAME}).
   */
  public static final String DEFAULT_HOSTNAME = "localhost";

  /**
   * The default password ({@value #DEFAULT_PASSWORD}).
   */
  public static final String DEFAULT_PASSWORD = "password";

  /**
   * The default username ({@value #DEFAULT_USERNAME}).
   */
  public static final String DEFAULT_USERNAME = "admin";

  private final Collection<String> filesToSend;

  private final String uri;

  private final String password;

  private final String topicname;

  private final String username;

  private static final Predicate<Void> isRunningWithSsl = Predicates.isRunningWithSsl();

  /**
   * Constructs a new MQTTProducer.
   * 
   * @param username username to use to connect to the message broker if not using SSL.
   * @param password password to use to connect to the message broker if not using SSL.
   * @param hostname hostname of the message broker.
   * @param port port the message broker server.
   * @param topicname name of a topic to send messages to if no queuename is set.
   * @param filesToSend a Collection of filenames whose contents will be sent to the broker.
   * @param environment the JNDI environment.
   */
  public MQTTProducer(String username, String password, String hostname, Integer port,
      String topicname, Collection<String> filesToSend) {
    this.username = username;
    this.password = password;
    this.topicname = topicname;
    this.filesToSend = filesToSend;
    if (isRunningWithSsl.test(null)) {
      uri = String.format("ssl://%s:%d", hostname, port);
    } else {
      uri = String.format("tcp://%s:%d", hostname, port);
    }
  }

  /**
   * Sends messages to a message broker over the MQTT protocol.
   */
  @Override
  public void run() {
    try {
      MqttClient client = new MqttClient(uri, "producers/MQTTProducer");
      MqttConnectOptions connOpts = new MqttConnectOptions();
      if (!isRunningWithSsl.test(null)) {
        connOpts.setUserName(username);
        connOpts.setPassword(password.toCharArray());
      }
      connOpts.setCleanSession(true);
      client.connect(connOpts);
      LOGGER.info(String.format("Connected to message broker at %s", uri));
      for (String fileToSend : filesToSend) {
        LOGGER.finer(String.format("Preparing to send %s ...", fileToSend));
        try {
          byte[] data = Files.readAllBytes(Paths.get(fileToSend));
          MqttMessage message = new MqttMessage(data);
          message.setQos(0);
          if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.finer(String.format("Sending message: %s", new String(data)));
          }
          client.publish(topicname, message);
          LOGGER.finer("Message sent OK");
        } catch (IOException e) {
          LOGGER.warning(String.format("Problem reading file %s: %s", fileToSend, e.getMessage()));
        }
      }
      client.disconnect();
    } catch (MqttException e) {
      LOGGER.log(Level.SEVERE, "Failure!", e);
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
   * <td>1883</td>
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
   * The only required option is {@code -topicname}. The program will not run if it is not
   * specified.
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
  public static void main(String[] args) {

    List<String> filesToSend = new ArrayList<>();
    String hostname = DEFAULT_HOSTNAME;
    String password = DEFAULT_PASSWORD;
    Integer port = null;
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

    Predicate<String> isNullOrEmpty = Predicates.isNull().or(Predicates.isEmpty());
    if (isNullOrEmpty.test(topicname)) {
      System.err.println("-topicname not set!");
      System.err.println("");
      showUsageAndExit(1);
    }

    if (port == null) {
      port = MQTT.getPort();
    }

    if (filesToSend.isEmpty()) {
      System.err.println("No files specified.");
      System.exit(1);
    }

    try {
      MQTTProducer producer =
          new MQTTProducer(username, password, hostname, port, topicname, filesToSend);
      producer.run();
    } catch (RuntimeException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }

  }

  private static void showUsageAndExit(int code) {
    showUsage();
    System.exit(code);
  }

  private static void showUsage() {
    System.err.printf("%s -topicname <name> [OPTION] FILE [FILE FILE]...%n",
        MQTTProducer.class.getName());
    System.err.println("");
    System.err.println("Sends the named file(s) to the specified topic.");
    System.err.println("");
    System.err.println("Options:");
    System.err.println("");
    System.err.println(" -help                 show this message and exit");
    System.err.println(" -host <hostname>      connect to the host running the message broker");
    System.err.println("                         (default is localhost)");
    System.err
        .println(" -password <arg>       message broker connection password (default is password)");
    System.err
        .println(" -port <port>          port message broker is running on (default is 1883)");
    System.err.println(" -topicname <name>     the topic to subscribe to");
    System.err
        .println(" -username <username>  message broker connection username (default is admin)");
    System.err.println("");
    System.err.println("Note:");
    System.err.println("");
    System.err.println("The -username and -password options are ignored when using SSL.");
  }

}
