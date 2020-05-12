package org.restfeeds.client;

public interface Lock {

  /**
   * Unlocks the lock.
   *
   * @throws IllegalStateException if the lock has already been unlocked.
   */
  void unlock();

  /**
   * @return if the lock is still valid.
   */
  boolean isValid();

}
