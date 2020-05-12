package org.restfeeds.client;

import java.time.Instant;
import java.util.Objects;

public class LockingParams {

  private final String name;

  private final Instant lockAtMostUntil;

  public LockingParams(String name, Instant lockAtMostUntil) {
    this.name = Objects.requireNonNull(name);
    this.lockAtMostUntil = Objects.requireNonNull(lockAtMostUntil);
    if (lockAtMostUntil.isBefore(Instant.now())) {
      throw new IllegalArgumentException("lockAtMostUntil is in the past for lock '" + name + "'.");
    }
    if (name.isEmpty()) {
      throw new IllegalArgumentException("lock name can not be empty");
    }
  }

  public String getName() {
    return name;
  }

  public Instant getLockAtMostUntil() {
    return lockAtMostUntil;
  }
}
