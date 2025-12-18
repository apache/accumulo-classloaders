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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LockInfoTest {

  @TempDir
  private static Path tempDir;

  @Test
  public void testLock() throws Exception {
    final Path fileToLock = tempDir.resolve("lock_file");
    final Path lockFile = tempDir.resolve("lock_file.lock");

    assertFalse(Files.exists(fileToLock));
    assertFalse(Files.exists(lockFile));
    final LockInfo lockInfo = LockInfo.lockFile(fileToLock);
    try {
      // the file to lock need not exist, but the lock should create the lockFile to reserve it
      assertFalse(Files.exists(fileToLock));
      assertTrue(Files.exists(lockFile));

      assertNotNull(lockInfo);
      assertTrue(lockInfo.getLock().acquiredBy().equals(lockInfo.getChannel()));
      assertFalse(lockInfo.getLock().isShared());
      assertTrue(lockInfo.getLock().isValid());

      // Test that another thread can't get the lock
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final Thread t = new Thread(() -> {
        try {
          assertNull(LockInfo.lockFile(fileToLock));
        } catch (IOException e) {
          error.set(e);
        }
      });
      t.start();
      t.join();
      assertNull(error.get());

    } finally {
      lockInfo.close();
    }
    // the lockFile doesn't get deleted when the lock is released
    assertFalse(Files.exists(fileToLock));
    assertTrue(Files.exists(lockFile));
  }

}
