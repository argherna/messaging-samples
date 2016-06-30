package com.github.argherna.messaging.core;

import static com.github.argherna.messaging.core.MessageProtocol.AMQP;
import static com.github.argherna.messaging.core.MessageProtocol.OPENWIRE;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Factories for Predicate and BiPredicate objects.
 * 
 * @author andy
 *
 */
public class Predicates {

  private Predicates() {}

  /**
   * Returns a BiPredicate that evaluates that checks if the given String array contains an element
   * after the given index.
   */
  public static BiPredicate<String[], Integer> hasElementAfter() {
    return (ar, i) -> ar.length > i;
  }

  /**
   * Returns a Predicate that checks if a given String is {@code null}.
   */
  public static Predicate<String> isNull() {
    return s -> s == null;
  }

  /**
   * Returns a BiPredicate that checks if the given Strings are both {@code null}.
   */
  public static BiPredicate<String, String> areNull() {
    return (s1, s2) -> s1 == null && s2 == null;
  }

  /**
   * Returns a Predicate that checks if a String is empty.
   * 
   * @see String#isEmpty()
   */
  public static Predicate<String> isEmpty() {
    return s -> s != null && s.isEmpty();
  }

  /**
   * Returns a BiPredicate that checks if the given Strings are empty.
   * 
   * @see String#isEmpty()
   */
  public static BiPredicate<String, String> areEmpty() {
    return (s1, s2) -> (s1 != null && s1.isEmpty()) && (s2 != null && s2.isEmpty());
  }

  /**
   * Returns a BiPredicate that checks if the given Strings are populated.
   * 
   * @see String#isEmpty()
   */
  public static BiPredicate<String, String> arePopulated() {
    return (s1, s2) -> (s1 != null && !s1.isEmpty()) && (s2 != null && !s2.isEmpty());
  }

  /**
   * Returns a Predicate that checks if the {@code javax.net.ssl.keyStore} system property is set.
   */
  public static Predicate<Void> isRunningWithSsl() {
    return v -> System.getProperty("javax.net.ssl.keyStore") != null;
  }

  /**
   * Returns a Predicate that checks if the given {@link MessageProtocol} has JMS support.
   */
  public static Predicate<MessageProtocol> isJmsSupported() {
    return p -> p == AMQP || p == OPENWIRE;
  }
}
