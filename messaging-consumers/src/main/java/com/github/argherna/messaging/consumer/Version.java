package com.github.argherna.messaging.consumer;

import java.util.function.Supplier;

/**
 * Supplies the version name for this application.
 * 
 * @author andy
 *
 */
final class Version implements Supplier<String>{

  @Override
  public String get() {
    return "1.0.0-SNAPSHOT";
  }
}
