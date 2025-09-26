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

import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

import org.apache.accumulo.classloader.lcc.Constants;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.accumulo.core.util.Pair;

public class CacheUtils {

  private static final Set<PosixFilePermission> CACHE_DIR_PERMS =
      EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE, GROUP_READ, OTHERS_READ);
  private static final FileAttribute<Set<PosixFilePermission>> PERMISSIONS =
      PosixFilePermissions.asFileAttribute(CACHE_DIR_PERMS);
  private static final String lockFileName = "lock_file";

  public static Path mkdir(Path p) throws ContextClassLoaderException {
    try {
      return Files.createDirectory(p, PERMISSIONS);
    } catch (FileAlreadyExistsException e) {
      return p;
    } catch (IOException e) {
      throw new ContextClassLoaderException(
          "Error creating cache directory: " + p.toFile().getAbsolutePath(), e);
    }
  }

  public static Path createBaseCacheDir() throws ContextClassLoaderException {
    final String prop = Constants.CACHE_DIR_PROPERTY;
    final String cacheDir = System.getProperty(prop);
    if (cacheDir == null) {
      throw new ContextClassLoaderException("System property " + prop + " not set.");
    }
    return mkdir(Paths.get(URI.create(cacheDir)));
  }

  public static Path createOrGetContextCacheDir(String contextName)
      throws ContextClassLoaderException {
    Path baseContextDir = createBaseCacheDir();
    return mkdir(baseContextDir.resolve(contextName));
  }

  public static Pair<FileChannel,FileLock> lockContextCacheDir(Path contextCacheDir)
      throws ContextClassLoaderException {
    Path lockFilePath = contextCacheDir.resolve(lockFileName);
    try {
      FileChannel channel = FileChannel.open(lockFilePath,
          EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE), PERMISSIONS);
      FileLock lock = channel.tryLock();
      if (lock == null) {
        // something else has the lock
        channel.close();
        return null;
      } else {
        return new Pair<>(channel, lock);
      }
    } catch (IOException e) {
      throw new ContextClassLoaderException("Error creating lock file in context cache directory "
          + contextCacheDir.toFile().getAbsolutePath(), e);
    }
  }

}
