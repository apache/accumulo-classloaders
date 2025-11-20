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
package org.apache.accumulo.classloader.lcc.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.classloader.lcc.Constants;
import org.apache.accumulo.classloader.lcc.cache.CacheUtils.LockInfo;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheUtilsTest {

  private static final Logger LOG = LoggerFactory.getLogger(CacheUtilsTest.class);

  @TempDir
  private static Path tempDir;

  @BeforeEach
  public void beforeEach() {
    String tmp = tempDir.resolve("base").toUri().toString();
    LOG.info("Setting cache base directory to {}", tmp);
    System.setProperty(Constants.CACHE_DIR_PROPERTY, tmp);
  }

  @AfterEach
  public void afterEach() {
    System.clearProperty(Constants.CACHE_DIR_PROPERTY);
  }

  @Test
  public void testPropertyNotSet() {
    System.clearProperty(Constants.CACHE_DIR_PROPERTY);
    ContextClassLoaderException ex =
        assertThrows(ContextClassLoaderException.class, () -> CacheUtils.createBaseCacheDir());
    assertEquals("Error getting classloader for context: System property "
        + Constants.CACHE_DIR_PROPERTY + " not set.", ex.getMessage());
  }

  @Test
  public void testCreateBaseDir() throws Exception {
    final Path base = Paths.get(tempDir.resolve("base").toUri());
    try {
      assertFalse(Files.exists(base));
      CacheUtils.createBaseCacheDir();
      assertTrue(Files.exists(base));
    } finally {
      Files.delete(base);
    }
  }

  @Test
  public void testCreateBaseDirMultipleTimes() throws Exception {
    final Path base = Paths.get(tempDir.resolve("base").toUri());
    try {
      assertFalse(Files.exists(base));
      CacheUtils.createBaseCacheDir();
      CacheUtils.createBaseCacheDir();
      CacheUtils.createBaseCacheDir();
      CacheUtils.createBaseCacheDir();
      assertTrue(Files.exists(base));
    } finally {
      Files.delete(base);
    }
  }

  @Test
  public void createOrGetContextCacheDir() throws Exception {
    final Path base = Paths.get(tempDir.resolve("base").toUri());
    try {
      assertFalse(Files.exists(base));
      CacheUtils.createOrGetContextCacheDir("context1");
      assertTrue(Files.exists(base));
      assertTrue(Files.exists(base.resolve("context1")));
      CacheUtils.createOrGetContextCacheDir("context2");
      assertTrue(Files.exists(base));
      assertTrue(Files.exists(base.resolve("context2")));
      CacheUtils.createOrGetContextCacheDir("context1");
      assertTrue(Files.exists(base));
      assertTrue(Files.exists(base.resolve("context1")));
    } finally {
      Files.delete(base.resolve("context1"));
      Files.delete(base.resolve("context2"));
      Files.delete(base);
    }
  }

  @Test
  public void testLock() throws Exception {
    final Path base = Paths.get(tempDir.resolve("base").toUri());
    final Path cx1 = base.resolve("context1");
    try {
      assertFalse(Files.exists(base));
      CacheUtils.createOrGetContextCacheDir("context1");
      assertTrue(Files.exists(base));
      assertTrue(Files.exists(cx1));

      final LockInfo lockInfo = CacheUtils.lockContextCacheDir(cx1);
      try {
        assertNotNull(lockInfo);
        assertTrue(lockInfo.getLock().acquiredBy().equals(lockInfo.getChannel()));
        assertFalse(lockInfo.getLock().isShared());
        assertTrue(lockInfo.getLock().isValid());

        // Test that another thread can't get the lock
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread t = new Thread(() -> {
          try {
            assertNull(CacheUtils.lockContextCacheDir(cx1));
          } catch (ContextClassLoaderException e) {
            error.set(e);
          }
        });
        t.start();
        t.join();
        assertNull(error.get());

      } finally {
        lockInfo.unlock();
      }
    } finally {
      Files.delete(cx1.resolve("lock_file"));
      Files.delete(cx1);
      Files.delete(base);
    }

  }

}
