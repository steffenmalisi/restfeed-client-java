package org.restfeeds.client;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * FeedReader which ensures that only one instance is running in parallel. It uses the {@link
 * LockProvider} to acquire a lock, which then is valid for the duration of
 * <code>failOverDuration</code>. After that duration the lock is released automatically and has
 * to be acquired again. Each instance of {@link FailOverSingleInstanceFeedReader} checks
 * periodically if the lock can be acquired to support failover after another process died
 * unexpectedly.
 * This class gives you the following guarantees:
 * <ul>
 *   <li>At-least-once delivery</li>
 *   <li>Preserved order of items</li>
 * </ul>
 */
public class FailOverSingleInstanceFeedReader extends FeedReader {

  private static final Logger logger = Logger
      .getLogger(FailOverSingleInstanceFeedReader.class.getName());

  private final LockProvider lockProvider;
  private final String lockName;
  private final Duration failOverDuration;
  private Lock lock;


  public FailOverSingleInstanceFeedReader(String feedBaseUrl,
      FeedItemConsumer consumer,
      FeedReaderRestClient feedReaderRestClient,
      NextLinkRepository nextLinkRepository,
      LockProvider lockProvider,
      String lockName,
      Duration failOverDuration) {
    super(feedBaseUrl, consumer, feedReaderRestClient, nextLinkRepository);
    this.lockProvider = lockProvider;
    this.lockName = lockName;
    this.failOverDuration = failOverDuration;
  }

  @Override
  protected boolean shouldRead() {
    if (alreadyLocked()) {
      logger.log(Level.FINE, "Already locked {0}", lockName);
      return true;
    }

    LockingParams lockingParams = new LockingParams(lockName,
        Instant.now().plusMillis(failOverDuration.toMillis()));

    // try to get a lock from the provider
    Optional<Lock> providerLock = lockProvider.lock(lockingParams);

    if (providerLock.isPresent()) {
      // lock could be acquired
      lock = providerLock.get();

      logger.log(Level.FINE, "Locked {0}, lock will be held at most until {1}",
          new Object[] {lockName, lockingParams.getLockAtMostUntil()});
      return true;

    } else {
      // lock could not be acquired
      logger.log(Level.FINE, "Not executing {0}. It is locked.", lockName);
      try {
        Thread.sleep(failOverDuration.toMillis());
      } catch (InterruptedException ie) {
        logger.log(Level.FINE, "Sleep was interrupted. Good morning.", ie);
      }
      return false;
    }
  }

  @Override
  protected boolean shouldAccept(String link, List<FeedItem> feedItems) {
    if (alreadyLocked()) {
      return true;
    } else {
      String currentLink = getNextLinkRepository().get(getFeedBaseUrl()).orElse(getFeedBaseUrl());
      if (!currentLink.equals(link)) {
        logger.log(Level.WARNING,
            "\nWhile having a lock at start of the process the lock is now gone and another process"
                + " updated the current position in the meantime."
                + "\nPlease check your configuration and possibly extend your lock time."
                + "\nExpected value: {0}"
                + "\nActual value: {1}",
            new Object[] {link, currentLink});
        return false;
      }
    }

    return true;
  }

  @Override
  protected void onAfterStop() {
    if (alreadyLocked()) {
      lock.unlock();
      lock = null;
      logger.log(Level.FINE, "Released lock {0}", lockName);
    }
  }

  private boolean alreadyLocked() {
    return lock != null && lock.isValid();
  }
}
