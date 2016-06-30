package com.github.argherna.messaging.consumer;

import static com.github.argherna.messaging.core.MessageProtocol.MQTT;
import static com.github.argherna.messaging.core.MessageProtocol.OPENWIRE;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.logging.Logger;

import com.github.argherna.messaging.core.MessageProtocol;
import com.github.argherna.messaging.core.Predicates;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

/**
 * Starts a Service that connects to a message broker and consumes messages.
 */
public class ConsumerService extends AbstractExecutionThreadService {

  private static final Logger LOGGER = Logger.getLogger(ConsumerService.class.getName());

  /**
   * The default host name ({@value #DEFAULT_HOSTNAME}).
   */
  public static final String DEFAULT_HOSTNAME = "localhost";

  /**
   * The default password ({@value #DEFAULT_PASSWORD}).
   */
  public static final String DEFAULT_PASSWORD = "password";

  /**
   * The default {@link MessageProtocol} (OPENWIRE).
   */
  public static final MessageProtocol DEFAULT_PROTOCOL = OPENWIRE;

  /**
   * The default username ({@value #DEFAULT_USERNAME}).
   */
  public static final String DEFAULT_USERNAME = "admin";

  /**
   * The default number of workers ({@value #DEFAULT_NUMBER_OF_WORKERS}).
   */
  public static final Integer DEFAULT_NUMBER_OF_WORKERS = 1;

  private ExecutorService executorService;

  private Worker worker;

  public ConsumerService(Worker worker) {
    this(MoreExecutors.newDirectExecutorService(), worker);
  }

  public ConsumerService(ExecutorService executorService, Worker worker) {
    this.executorService = executorService;
    this.worker = worker;
  }

  @Override
  protected void startUp() throws Exception {
    worker.startUp();
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      Future<?> f = executorService.submit(worker);

      // Block here until a message is received and processed.
      @SuppressWarnings("unused")
      Object unused = f.get();
      LOGGER.fine("Worker executed");
    }
  }

  @Override
  protected void triggerShutdown() {
    worker.triggerShutdown();
    boolean normalShutdown =
        MoreExecutors.shutdownAndAwaitTermination(executorService, 5l, TimeUnit.SECONDS);
    if (!normalShutdown) {
      LOGGER.warning("Failed to shutdown executor.");
    }
  }

  @Override
  protected void shutDown() throws Exception {
    worker.shutDown();
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
   * <td>Depends on protocol specified (see below)</td>
   * </tr>
   * <tr>
   * <td>{@code -protocol <protocol>}</td>
   * <td>Message protocol to use</td>
   * <td>openwire</td>
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
   * <tr>
   * <td>{@code -version}</td>
   * <td>Print the version and exit</td>
   * <td>N/A</td>
   * </tr>
   * </tbody>
   * </table>
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
   * <h3>Protocols</h3>
   * 
   * <p>
   * The table below shows what protocols are supported. Their default port number is specified but
   * any of these may be overridden by the {@code -port} option from the command line.
   * </p>
   * 
   * <table>
   * <thead>
   * <tr>
   * <th>Protocol Name</th>
   * <th>Default Port</th>
   * <th>Default SSL Port</th>
   * </tr>
   * </thead>
   * <tbody>
   * <tr>
   * <td>openwire</td>
   * <td>61616</td>
   * <td>61617</td>
   * </tr>
   * <tr>
   * <td>amqp</td>
   * <td>5672</td>
   * <td>5673</td>
   * </tr>
   * <tr>
   * <td>mqtt</td>
   * <td>1883</td>
   * <td>8883</td>
   * </tr>
   * </tbody>
   * </table>
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
   * <dd>Program was run with the {@code -help} or {@code -version} option specified.</dd>
   * <dl>
   * 
   * @param args the command line arguments
   */
  public static void main(String... args) {

    String hostname = DEFAULT_HOSTNAME;
    String password = DEFAULT_PASSWORD;
    Integer port = null;
    MessageProtocol protocol = DEFAULT_PROTOCOL;
    String queuename = null;
    String topicname = null;
    String username = DEFAULT_USERNAME;
    Integer workers = DEFAULT_NUMBER_OF_WORKERS;

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

        case "-version":
          showVersionAndExit();
          break;

        default:
          try {
            workers = Integer.valueOf(arg);
          } catch (NumberFormatException e) {
            System.err.printf("%s not a valid number of workers, defaulting to 1.%n", arg);
            workers = 1;
          }
          break;

      }
      argIdx++;
    }

    BiPredicate<String, String> areNullOrEmpty = Predicates.areNull().or(Predicates.areEmpty());
    BiPredicate<String, String> arePopulated = Predicates.arePopulated();
    if (areNullOrEmpty.test(topicname, queuename)) {
      System.err.println("Must set one of either -queuename or -topicname");
      System.err.println("");
      showUsageAndExit(1);
    } else if (arePopulated.test(topicname, queuename)) {
      System.err.println("Must set only one of either -queuename or -topicname");
      System.err.println("");
      showUsageAndExit(1);
    }

    Predicate<String> isNullOrEmpty = Predicates.isNull().or(Predicates.isEmpty());
    if (protocol == MQTT && isNullOrEmpty.test(topicname)) {
      System.err.println("Error: MQTT protocol needs a -topicname set!");
      System.exit(1);
    }

    if (port == null) {
      port = protocol.getPort();
    }

    try {
      Worker w =
          Workers.getWorker(protocol, username, password, hostname, port, queuename, topicname);
      final Service consumerService =
          workers > 1 ? new ConsumerService(Executors.newFixedThreadPool(workers), w)
              : new ConsumerService(w);
      consumerService.addListener(new ServiceStatusLogger(), MoreExecutors.directExecutor());
      consumerService.startAsync();

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          consumerService.stopAsync();
          try {
            consumerService.awaitTerminated(10l, TimeUnit.SECONDS);
          } catch (TimeoutException e) {
            System.err.println(">>>> Timed out waiting for ConsumerService to shut down!");
            e.printStackTrace();
          }
        }
      });
    } catch (Exception e) {
      System.err.println(">>>> Error while running!");
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void showVersionAndExit() {
    Version v = new Version();
    System.err.printf("ConsumerService v%s %n", v.get());
    System.err.printf("Java version: %s, vendor: %s%n", System.getProperty("java.version"),
        System.getProperty("java.vendor"));
    System.exit(2);
  }

  private static void showUsageAndExit(int code) {
    showUsage();
    System.exit(code);
  }

  private static void showUsage() {
    System.err.printf("%s -queuename <name> | -topicname <name> [OPTION] [WORKERS]...%n",
        ConsumerService.class.getName());
    System.err.println("");
    System.err.println("Starts the consumer service");
    System.err.println("");
    System.err.println("Arguments:");
    System.err.println("");
    System.err.println(" WORKERS               number of concurrent workers");
    System.err.println("");
    System.err.println("Options:");
    System.err.println("");
    System.err.println(" -help                 show this message and exit");
    System.err.println(" -host <hostname>      connect to the host running the message broker");
    System.err.println("                         (default is localhost)");
    System.err
        .println(" -password <arg>         message broker connection password (default is password)");
    System.err
        .println(" -port <port>          port message broker is running on (default is 61616)");
    System.err.println(" -protocol <protocol>  message protocol to use (default is openwire)");
    System.err.println(" -queuename <name>     the queue to subscribe to");
    System.err.println(" -topicname <name>     the topic to subscribe to");
    System.err
        .println(" -username <username>  message broker connection username (default is admin)");
    System.err.println(" -version              print the version and exit");
    System.err.println("");
    System.err.println("Note:");
    System.err.println("");
    System.err.println("The -username and -password options are ignored when using SSL.");
    System.err
        .println("A -queuename or -topicname must be set; it is an error to set both or neither.");
  }
}
