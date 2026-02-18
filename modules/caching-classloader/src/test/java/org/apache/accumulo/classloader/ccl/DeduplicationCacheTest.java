/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.classloader.ccl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.RemovalCause;

public class DeduplicationCacheTest {

  @Test
  public void testCollectionNotification() throws Exception {

    final var endBackgroundThread = new CountDownLatch(1);
    final var removalCauseQueue = new LinkedBlockingQueue<RemovalCause>();

    final var cache = new DeduplicationCache<String,Object,Object>((k, p) -> new Object(),
        Duration.ofSeconds(1), (key, value, cause) -> removalCauseQueue.add(cause));

    var createdCacheEntry = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      var value = cache.computeIfAbsent("TEST", () -> new Object());
      createdCacheEntry.countDown();
      try {
        // hold a strong reference in the background thread long enough to check the weak cache
        endBackgroundThread.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      assertNotNull(value);
    });
    t.start();
    createdCacheEntry.await();

    assertTrue(cache.anyMatch("TEST"::equals));

    // wait for expiration from strong cache
    assertEquals(RemovalCause.EXPIRED, removalCauseQueue.take());
    assertTrue(removalCauseQueue.isEmpty());

    // should still exist in the weak values cache
    assertTrue(cache.anyMatch("TEST"::equals));
    endBackgroundThread.countDown();

    t.join();

    // wait for it to be garbage collected (checking the cache triggers cleanup)
    while (cache.anyMatch("TEST"::equals)) {
      System.gc();
      Thread.sleep(50);
    }
    assertEquals(RemovalCause.COLLECTED, removalCauseQueue.take());
    assertTrue(removalCauseQueue.isEmpty());
  }

}
