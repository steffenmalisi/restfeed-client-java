package org.restfeeds.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.restfeeds.client.TestUtils.DummyFeedReaderRestClient;
import org.restfeeds.client.TestUtils.InMemoryListFeedItemConsumer;
import org.restfeeds.client.TestUtils.OrderedByIdEmitter;
import org.restfeeds.client.TestUtils.OrderedByItemIdFeedReaderRestClient;
import org.restfeeds.client.TestUtils.SimpleLockProvider;
import org.restfeeds.client.TestUtils.SynchronizedInMemoryNextLinkRepository;
import org.restfeeds.client.TestUtils.ThreadFacade;
import org.slf4j.bridge.SLF4JBridgeHandler;


public class FailOverSingleInstanceFeedReaderTests {

  private static final Logger logger = Logger
      .getLogger(FailOverSingleInstanceFeedReaderTests.class.getName());

  @BeforeAll
  static void setupLogger() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.setLevel(Level.FINEST);
  }

  @Test
  void singleInstanceShouldRead() throws InterruptedException {
    String feedBaseUrl = "http://localhost/events";
    InMemoryNextLinkRepository nextLinkRepository = new InMemoryNextLinkRepository();
    AtomicInteger count = new AtomicInteger(0);
    FeedReader feedReader =
        new FailOverSingleInstanceFeedReader(
            feedBaseUrl,
            feedItem -> count.incrementAndGet(),
            new DummyFeedReaderRestClient(),
            nextLinkRepository,
            new SimpleLockProvider(1),
            "LOCK_NAME",
            Duration.ofMinutes(5));

    new Thread(feedReader::read).start();

    Thread.sleep(300L);
    assertTrue(count.get() >= 2);
    assertEquals("http://localhost/events?offset=100",
        nextLinkRepository.get(feedBaseUrl).orElse(null));
  }

  @Test
  void secondInstanceShouldNotRead() throws InterruptedException {
    String feedBaseUrl = "";
    InMemoryNextLinkRepository nextLinkRepository = new SynchronizedInMemoryNextLinkRepository(1);
    SimpleLockProvider simpleLockProvider = new SimpleLockProvider(1);
    String lockName = "TEST_FEED_READER";

    AtomicInteger count1 = new AtomicInteger(0);
    FeedReader feedReader1 =
        new FailOverSingleInstanceFeedReader(
            feedBaseUrl,
            feedItem -> count1.incrementAndGet(),
            new OrderedByItemIdFeedReaderRestClient(10,100),
            nextLinkRepository,
            simpleLockProvider,
            lockName,
            Duration.ofMinutes(5));

    // should not read, because failOverDuration is 5 Minutes
    AtomicInteger count2 = new AtomicInteger(0);
    FeedReader feedReader2 =
        new FailOverSingleInstanceFeedReader(
            feedBaseUrl,
            feedItem -> count2.incrementAndGet(),
            new OrderedByItemIdFeedReaderRestClient(10,1),
            nextLinkRepository,
            simpleLockProvider,
            lockName,
            Duration.ofMinutes(5));

    // Start the first reader
    new Thread(feedReader1::read).start();

    // Wait a few millis for the first reader to start
    Thread.sleep(50L);

    // Start the second reader
    new Thread(feedReader2::read).start();

    // Let them read a while
    Thread.sleep(300L);

    assertTrue(count1.get() >= 2, "Reader 1 should have consumed.");
    assertEquals(0, count2.get(), "Reader 2 should not have consumed.");
  }

  @Test
  void secondInstanceShouldReadAfterFailover() throws InterruptedException {
    secondInstanceShouldReadAfterFailoverInternal(200, 200, 50, 600, 150, 150, 150, 150);
    secondInstanceShouldReadAfterFailoverInternal(500, 500, 100, 1700, 10, 200, 10, 10);
  }

  @Test
  @Disabled("Used for manual execution. This is because of high number of random executions, which may take long time.")
  void secondInstanceShouldReadAfterFailoverWithRandomlyConfiguredTimes()
      throws InterruptedException {
    for (int i = 0; i < 1000; i++) {
      Random random = new Random();
      List<Long> randoms = random.longs(8L, 0L, 1000L).boxed().collect(Collectors.toList());
      secondInstanceShouldReadAfterFailoverInternal(randoms.get(0), randoms.get(1), randoms.get(2),
          randoms.get(3) + 500, randoms.get(4), randoms.get(5), randoms.get(6), randoms.get(7));

      // wait a few millis until all Threads are stopped
      Thread.sleep(500L);
    }


  }

  private void secondInstanceShouldReadAfterFailoverInternal(
      long firstInstanceFailOverDurationMillis, long secondInstanceFailOverDurationMillis,
      long millisToStartSecondInstance,
      long millisToInterruptFirstInstance, long consumerMaxProcessingMillis,
      long restClientMaxProcessingMillis, long nextLinkRepositoryMaxProcessingMillis,
      long lockMaxProcessingMillis) throws InterruptedException {

    logger.log(Level.INFO,
        "**************************************************************************");

    logger.log(Level.INFO,
        "Starting secondInstanceShouldReadAfterFailover with {0}, {1}, {2}, {3}",
        new Object[] {firstInstanceFailOverDurationMillis, secondInstanceFailOverDurationMillis,
            millisToStartSecondInstance,
            millisToInterruptFirstInstance});

    logger.log(Level.INFO,
        "**************************************************************************");

    AtomicInteger shouldReadCallCount1 = new AtomicInteger(0);
    AtomicInteger shouldReadCallCount2 = new AtomicInteger(0);

    final String feedBaseUrl = "";
    final InMemoryListFeedItemConsumer feedItemConsumer = new InMemoryListFeedItemConsumer(
        consumerMaxProcessingMillis);
    final OrderedByItemIdFeedReaderRestClient restClient = new OrderedByItemIdFeedReaderRestClient(
        10, restClientMaxProcessingMillis);
    final InMemoryNextLinkRepository nextLinkRepository = new SynchronizedInMemoryNextLinkRepository(
        nextLinkRepositoryMaxProcessingMillis);
    final SimpleLockProvider simpleLockProvider = new SimpleLockProvider(lockMaxProcessingMillis);
    String lockName = "TEST_FEED_READER";

    FeedReader feedReader1 =
        new FailOverSingleInstanceFeedReader(
            feedBaseUrl,
            feedItemConsumer,
            restClient,
            nextLinkRepository,
            simpleLockProvider,
            lockName,
            Duration.ofMillis(firstInstanceFailOverDurationMillis)) {

          @Override
          protected boolean shouldRead() {
            shouldReadCallCount1.incrementAndGet();
            return super.shouldRead();
          }

        };

    FeedReader feedReader2 =
        new FailOverSingleInstanceFeedReader(
            feedBaseUrl,
            feedItemConsumer,
            restClient,
            nextLinkRepository,
            simpleLockProvider,
            lockName,
            Duration.ofMillis(secondInstanceFailOverDurationMillis)) {

          @Override
          protected boolean shouldRead() {
            shouldReadCallCount2.incrementAndGet();
            return super.shouldRead();
          }

        };

    // Start the first reader
    ThreadFacade thread1 = new ThreadFacade(feedReader1);
    thread1.start();
    // Wait a few millis to start the second one with a delay
    Thread.sleep(millisToStartSecondInstance);
    // Start the second reader
    ThreadFacade thread2 = new ThreadFacade(feedReader2);
    thread2.start();

    // Let them read a while
    Thread.sleep(millisToInterruptFirstInstance);
    // Interrupt the first thread, makes it possible for the second one to take over
    thread1.interrupt();

    // Let thread 2 read a while
    Thread.sleep(500);

    assertTrue(shouldReadCallCount1.get() >= 1, "Consumer 1 should have run.");
    assertTrue(shouldReadCallCount2.get() >= 1, "Consumer 2 should have run.");

    List<FeedItem> consumedItems = feedItemConsumer.getConsumedItems();

    logger.log(Level.INFO,
        "**************************************************************************");
    logger.log(Level.INFO,
        "Consumed {0} items", consumedItems.size());
    logger.log(Level.INFO,
        "**************************************************************************");

    assertCorrectOrder(consumedItems);

    assertAtLeastOnceDelivery(consumedItems, restClient);

    thread2.interrupt();
  }

  private static void assertCorrectOrder(List<FeedItem> consumedItems) {
    for (int i = 0; i < consumedItems.size(); i++) {
      if (i != 0) {
        int prevId = Integer.parseInt(consumedItems.get(i - 1).getId());
        int currId = Integer.parseInt(consumedItems.get(i).getId());
        assertTrue(currId - prevId == 0 || currId - prevId == 1,
            String.format(
                "Previous items order %s has to be less or equal than current items order %s.",
                prevId, currId));
      }
    }

  }

  private static void assertAtLeastOnceDelivery(List<FeedItem> consumedItems,
      OrderedByIdEmitter emitter) {

    Map<Integer, List<FeedItem>> consumedFeedItemsById = mapItemsById(consumedItems);
    Map<Integer, List<FeedItem>> emittedFeedItemsById = mapItemsById(emitter.getEmittedItems());

    // for the last emitted item, it could be possible that the consumer did not fully process it
    // until it was interrupted. Therefore just checking for all items exclusive the maxEmittedItemId
    for (int i = 0; i < emitter.getMaxEmittedItemId(); i++) {
      assertTrue(emittedFeedItemsById.containsKey(i) && consumedFeedItemsById.containsKey(i),
          "Emitted item has to be at least once consumed.");
    }

  }

  private static Map<Integer, List<FeedItem>> mapItemsById(List<FeedItem> input) {
    Map<Integer, List<FeedItem>> result = new HashMap<>();
    for (FeedItem feedItem : input) {
      int feedItemId = Integer.parseInt(feedItem.getId());
      List<FeedItem> feedItemsById = result
          .computeIfAbsent(feedItemId, k -> new ArrayList<>());
      feedItemsById.add(feedItem);
    }
    return result;
  }


}
