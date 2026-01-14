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
package org.apache.accumulo.classloader.lcc.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

public class DeduplicationCacheTest {

  private static final Logger LOG = LoggerFactory.getLogger(DeduplicationCacheTest.class);

  @Test
  public void testCollectionNotification() throws Exception {

    final AtomicBoolean listenerCalled = new AtomicBoolean(false);

    final RemovalListener<String,URLClassLoader> removalListener = new RemovalListener<>() {
      @Override
      public void onRemoval(@Nullable String key, @Nullable URLClassLoader value,
          RemovalCause cause) {
        LOG.info("Entry removed due to {}. K = {}, V = {}", cause, key, value);
        if (cause == RemovalCause.COLLECTED) {
          listenerCalled.set(true);
        }
      }
    };

    final DeduplicationCache<String,URL[],URLClassLoader> cache = new DeduplicationCache<>(
        LccUtils::createClassLoader, Duration.ofSeconds(5), removalListener);

    final URL jarAOrigLocation =
        DeduplicationCacheTest.class.getResource("/ClassLoaderTestA/TestA.jar");
    assertNotNull(jarAOrigLocation);

    Thread t = new Thread(() -> {
      cache.computeIfAbsent("TEST", () -> new URL[] {jarAOrigLocation});
    });
    t.start();
    t.join();

    boolean exists = cache.anyMatch((k) -> k.equals("TEST"));
    assertTrue(exists);

    Thread.sleep(10_000); // sleep twice as long as the access time duration in the strong reference
                          // cache

    exists = cache.anyMatch((k) -> k.equals("TEST"));
    assertTrue(exists); // This is true because it's coming from the weak reference cache

    Thread.sleep(10_000); // sleep twice as long as the access time duration in the strong reference
                          // cache
    System.gc();

    exists = cache.anyMatch((k) -> k.equals("TEST"));
    assertFalse(exists);
    assertTrue(listenerCalled.get());

  }

}
