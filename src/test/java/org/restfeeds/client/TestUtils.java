package org.restfeeds.client;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;

public class TestUtils {

  static class InMemoryListFeedItemConsumer implements FeedItemConsumer {

    private static long DEFAULT_MAX_PROCESSING_DURATION = 500;

    private List<FeedItem> consumedItems = new ArrayList<>();
    private long maxProcessingDurationMillis;

    public InMemoryListFeedItemConsumer() {
      this(DEFAULT_MAX_PROCESSING_DURATION);
    }

    public InMemoryListFeedItemConsumer(long maxProcessingDurationMillis) {
      this.maxProcessingDurationMillis = maxProcessingDurationMillis;
    }


    @Override
    synchronized public void accept(FeedItem feedItem) {
      try {
        OptionalLong random = new Random().longs(1, 0, maxProcessingDurationMillis).findFirst();
        long processingDuration = random.orElseGet(() -> DEFAULT_MAX_PROCESSING_DURATION);
        Thread.sleep(processingDuration);
      } catch (InterruptedException ignored) {
      }
      consumedItems.add(feedItem);
    }

    public List<FeedItem> getConsumedItems() {
      return consumedItems;
    }
  }

  static class SynchronizedInMemoryNextLinkRepository extends InMemoryNextLinkRepository {

    private static long DEFAULT_MAX_PROCESSING_DURATION = 500;

    private long maxProcessingDurationMillis;

    public SynchronizedInMemoryNextLinkRepository() {
      this(DEFAULT_MAX_PROCESSING_DURATION);
    }

    public SynchronizedInMemoryNextLinkRepository(long maxProcessingDurationMillis) {
      this.maxProcessingDurationMillis = maxProcessingDurationMillis;
    }

    @Override
    synchronized public void save(String feed, String nextLink) {
      try {
        OptionalLong random = new Random().longs(1, 0, maxProcessingDurationMillis).findFirst();
        long processingDuration = random.orElseGet(() -> DEFAULT_MAX_PROCESSING_DURATION);
        Thread.sleep(processingDuration);
      } catch (InterruptedException ignored) {
      }
      super.save(feed, nextLink);
    }
  }

  static class DummyFeedReaderRestClient implements FeedReaderRestClient {

    @Override
    public List<FeedItem> getFeedItems(String feedUrl) {
      try {
        Thread.sleep(100L);
      } catch (InterruptedException ignored) {
      }
      List<FeedItem> feedItems = new java.util.ArrayList<>();
      FeedItem feedItem = new FeedItem();
      feedItem.setNext("/events?offset=100");
      feedItems.add(feedItem);
      return feedItems;
    }
  }

  interface OrderedByIdEmitter {

    List<FeedItem> getEmittedItems();

    int getMaxEmittedItemId();
  }

  static class OrderedByItemIdFeedReaderRestClient implements FeedReaderRestClient,
      OrderedByIdEmitter {

    private static long DEFAULT_MAX_PROCESSING_DURATION = 500;

    private List<FeedItem> feedItems = new ArrayList<>();
    private List<FeedItem> emittedItems = new ArrayList<>();
    private int maxEmittedItemId;
    private long maxProcessingDurationMillis;

    public OrderedByItemIdFeedReaderRestClient() {
      this(10, DEFAULT_MAX_PROCESSING_DURATION);
    }

    public OrderedByItemIdFeedReaderRestClient(int maxNumberOfItems,
        long maxProcessingDurationMillis) {
      this.maxProcessingDurationMillis = maxProcessingDurationMillis;
      for (int i = 0; i < maxNumberOfItems; i++) {
        FeedItem item = new FeedItem();
        item.setId(String.valueOf(i));
        item.setNext(String.valueOf(i + 1));
        feedItems.add(item);
      }
    }

    @Override
    synchronized public List<FeedItem> getFeedItems(String feedUrl) {
      try {
        OptionalLong random = new Random().longs(1, 0, maxProcessingDurationMillis).findFirst();
        long processingDuration = random.orElseGet(() -> DEFAULT_MAX_PROCESSING_DURATION);
        Thread.sleep(processingDuration);
      } catch (InterruptedException ignored) {
      }
      if (feedUrl.isEmpty()) {
        onEmitting(feedItems.get(0));
        return Collections.singletonList(feedItems.get(0));
      } else {
        int next = Integer.parseInt(feedUrl);
        if (next >= feedItems.size()) {
          return Collections.emptyList();
        } else {
          onEmitting(feedItems.get(next));
          return Collections.singletonList(feedItems.get(next));
        }
      }
    }

    private void onEmitting(FeedItem feedItem) {
      emittedItems.add(feedItem);
      int itemId = Integer.parseInt(feedItem.getId());
      if (itemId > maxEmittedItemId) {
        maxEmittedItemId = itemId;
      }
    }

    @Override
    public List<FeedItem> getEmittedItems() {
      return emittedItems;
    }

    @Override
    public int getMaxEmittedItemId() {
      return maxEmittedItemId;
    }
  }

  static class SimpleLockProvider implements LockProvider {

    private static long DEFAULT_MAX_PROCESSING_DURATION = 500;

    private boolean locked;
    private Instant lockedUntil;
    private long maxProcessingDurationMillis;

    public SimpleLockProvider() {
      this(DEFAULT_MAX_PROCESSING_DURATION);
    }

    public SimpleLockProvider(long maxProcessingDurationMillis) {
      this.maxProcessingDurationMillis = maxProcessingDurationMillis;
    }

    @Override
    synchronized public Optional<Lock> lock(LockingParams lockingParams) {
      try {
        OptionalLong random = new Random().longs(1, 0, maxProcessingDurationMillis).findFirst();
        long processingDuration = random.orElseGet(() -> DEFAULT_MAX_PROCESSING_DURATION);
        Thread.sleep(processingDuration);
      } catch (InterruptedException ignored) {
      }
      if (!locked || Instant.now().isAfter(lockedUntil)) {
        locked = true;
        lockedUntil = lockingParams.getLockAtMostUntil();
        return Optional.of(new SimpleLock(lockingParams));
      } else {
        return Optional.empty();
      }
    }
  }

  static class SimpleLock implements Lock {

    private LockingParams params;
    private boolean unlocked = false;

    public SimpleLock(LockingParams params) {
      this.params = params;
    }

    @Override
    public void unlock() {
      unlocked = true;
    }

    @Override
    public boolean isValid() {
      return !unlocked && !Instant.now().isAfter(params.getLockAtMostUntil());
    }
  }

  static class ThreadFacade implements Runnable {

    private Thread worker;
    private FeedReader target;

    public ThreadFacade(FeedReader target) {
      this.target = target;
    }

    public void start() {
      worker = new Thread(this);
      worker.start();
    }

    public void stop() {
      target.stop();
    }

    public void interrupt() {
      stop();
      worker.interrupt();
    }

    public void run() {
      target.read();
    }
  }


}
