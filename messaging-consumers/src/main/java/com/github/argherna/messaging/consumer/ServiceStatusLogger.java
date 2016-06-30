package com.github.argherna.messaging.consumer;

import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.Listener;
import com.google.common.util.concurrent.Service.State;

/**
 * A Service Listener that Logs messages to a {@linkplain java.util.logging.Logger Java Logger}.
 * 
 * @author andy
 *
 */
class ServiceStatusLogger extends Listener {

  private static final Logger LOGGER = Logger.getLogger(ServiceStatusLogger.class.getName());

  /**
   * {@inheritDoc}
   * 
   * Logs service is starting at {@linkplain Level#INFO INFO} level. Message includes version
   * number.
   */
  @Override
  public void starting() {
    LOGGER.info(new StartupMessage());
  }

  /**
   * {@inheritDoc}
   * 
   * Logs service is running at {@linkplain Level#INFO INFO} level.
   */
  @Override
  public void running() {
    LOGGER.info("Service is running.");
  }

  /**
   * {@inheritDoc}
   * 
   * Logs service is stopping at {@linkplain Level#WARNING WARNING} level. Message includes previous
   * service state.
   */
  @Override
  public void stopping(State from) {
    LOGGER.warning(String.format("Service STOPPING (trasitioning from %s)!", from));
  }

  /**
   * {@inheritDoc}
   * 
   * Logs service has terminated at {@linkplain Level#INFO INFO} level. Message includes previous
   * service state.
   */
  @Override
  public void terminated(State from) {
    LOGGER.info(String.format("Service TERMINATED from %s.", from));
  }

  /**
   * {@inheritDoc}
   * 
   * Logs service has failed at {@linkplain Level#SEVERE SEVERE} level. The associated Throwable is
   * logged too.
   */
  @Override
  public void failed(State from, Throwable failure) {
    LogRecord record =
        new LogRecord(Level.SEVERE,
            String.format("Service going into FAILURE state from %s!", from));
    record.setThrown(failure);
    LOGGER.log(record);
  }

  /**
   * Supplies the startup message.
   * 
   * @author andy
   *
   */
  private static class StartupMessage implements Supplier<String> {

    private static final Supplier<String> VERSION = new Version();

    @Override
    public String get() {
      return String.format("%s ConsumerService version %s", Service.State.STARTING, VERSION.get());
    }

  }
}
