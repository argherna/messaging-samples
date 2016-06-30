package com.github.argherna.messaging.consumer;

import com.google.common.util.concurrent.Service;

/**
 * Performs work for a {@linkplain Service}.
 * 
 * <p>
 * Work is performed in the {@link Runnable#run() run} method. Lifecycle methods are called by the
 * Service object's lifecycle methods.
 * </p>
 * 
 * @author andy
 *
 */
public interface Worker extends Runnable {

  /**
   * Called when a Service is starting.
   * 
   * @throws Exception
   */
  public void startUp() throws Exception;

  /**
   * Called when a Service needs to shut down.
   */
  public void triggerShutdown();

  /**
   * Called when a Service has initiated its shutdown.
   * 
   * @throws Exception
   */
  public void shutDown() throws Exception;
}
