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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.util.EnumSet;

public class LockInfo implements AutoCloseable {

  private static final String SUFFIX = ".lock";
  private final FileChannel channel;
  private final FileLock lock;

  /**
   * Acquire an exclusive lock for manipulating the specified file. This will create a lock file for
   * the specified path, using the same path name, with ".lock" appended to the end. Returns null if
   * lock can not be acquired. Caller MUST call LockInfo.unlock when done manipulating files locked
   * by this lock.
   */
  public static LockInfo lockFile(final Path path) throws IOException {
    final var lockPath = Path.of(path.toAbsolutePath() + SUFFIX);
    final var channel = FileChannel.open(lockPath, EnumSet.of(CREATE, WRITE));
    final FileLock lock;
    try {
      lock = channel.tryLock();
      if (lock != null) {
        return new LockInfo(channel, lock);
      }
    } catch (OverlappingFileLockException e) {
      // another thread has the lock
    }
    // another thread or another process (lock was null) has the lock
    channel.close();
    return null;
  }

  private LockInfo(final FileChannel channel, final FileLock lock) {
    this.channel = requireNonNull(channel, "channel must be supplied");
    this.lock = requireNonNull(lock, "lock must be supplied");
  }

  FileChannel getChannel() {
    return channel;
  }

  FileLock getLock() {
    return lock;
  }

  @Override
  public void close() throws IOException {
    lock.release();
    channel.close();
  }

}
