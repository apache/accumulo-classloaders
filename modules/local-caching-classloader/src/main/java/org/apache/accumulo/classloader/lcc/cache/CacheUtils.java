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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.classloader.lcc.Constants;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.classloader.lcc.util.URLClassLoaderHelper;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Retry.RetryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CacheUtils.class);

  private static final Set<PosixFilePermission> CACHE_DIR_PERMS =
      EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE);
  private static final FileAttribute<Set<PosixFilePermission>> PERMISSIONS =
      PosixFilePermissions.asFileAttribute(CACHE_DIR_PERMS);
  private static final String lockFileName = "lock_file";

  public static class LockInfo {

    private final FileChannel channel;
    private final FileLock lock;

    public LockInfo(FileChannel channel, FileLock lock) {
      this.channel = requireNonNull(channel, "channel must be supplied");
      this.lock = requireNonNull(lock, "lock must be supplied");
    }

    FileChannel getChannel() {
      return channel;
    }

    FileLock getLock() {
      return lock;
    }

    public void unlock() throws IOException {
      lock.release();
      channel.close();
    }

  }

  public static void createBaseDirs(final Path baseDir)
      throws IOException, ContextClassLoaderException {
    if (baseDir == null) {
      throw new ContextClassLoaderException("received null for cache directory");
    }
    var contextDir = baseDir.resolve("contexts");
    Files.createDirectories(contextDir, PERMISSIONS);
    var resourcesDir = baseDir.resolve("resources");
    Files.createDirectories(resourcesDir, PERMISSIONS);
  }

  public static Path contextsDir(final Path baseCacheDir)
      throws IOException, ContextClassLoaderException {
    createBaseDirs(baseCacheDir);
    return baseCacheDir.resolve("contexts");
  }

  public static Path resourcesDir(final Path baseCacheDir)
      throws IOException, ContextClassLoaderException {
    createBaseDirs(baseCacheDir);
    return baseCacheDir.resolve("resources");
  }

  /**
   * Acquire an exclusive lock on the "lock_file" file in the context cache directory. Returns null
   * if lock can not be acquired. Caller MUST call LockInfo.unlock when done manipulating the cache
   * directory
   */
  public static LockInfo lockContextCacheDir(final Path contextCacheDir)
      throws ContextClassLoaderException {
    final Path lockFilePath = contextCacheDir.resolve(lockFileName);
    try {
      final FileChannel channel = FileChannel.open(lockFilePath,
          EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE), PERMISSIONS);
      try {
        final FileLock lock = channel.tryLock();
        if (lock == null) {
          // something else has the lock
          channel.close();
          return null;
        } else {
          return new LockInfo(channel, lock);
        }
      } catch (OverlappingFileLockException e) {
        // something else has the lock
        channel.close();
        return null;
      }
    } catch (IOException e) {
      throw new ContextClassLoaderException("Error creating lock file in context cache directory "
          + contextCacheDir.toFile().getAbsolutePath(), e);
    }
  }

  public static URLClassLoaderHelper stageContext(final Path baseCacheDir,
      final ContextDefinition contextDefinition)
      throws IOException, ContextClassLoaderException, InterruptedException, URISyntaxException {
    final RetryFactory retryFactory =
        Retry.builder().infiniteRetries().retryAfter(1, TimeUnit.SECONDS)
            .incrementBy(1, TimeUnit.SECONDS).maxWait(5, TimeUnit.MINUTES).backOffFactor(2)
            .logInterval(1, TimeUnit.SECONDS).createFactory();
    requireNonNull(contextDefinition, "definition must be supplied");
    CacheUtils.createBaseDirs(baseCacheDir);
    Path contextsDir = CacheUtils.contextsDir(baseCacheDir);
    final Set<File> localFiles = new LinkedHashSet<>();
    try {
      LockInfo lockInfo = CacheUtils.lockContextCacheDir(contextsDir);
      Files.write(
          contextsDir
              .resolve(contextDefinition.getContextName() + "_" + contextDefinition.getChecksum()),
          contextDefinition.toJson().getBytes(UTF_8));
      while (lockInfo == null) {
        // something else is updating this directory
        LOG.info("Directory {} locked, another process must be updating the class loader contents. "
            + "Retrying in 1 second", contextsDir);
        Thread.sleep(1000);
        lockInfo = CacheUtils.lockContextCacheDir(contextsDir);
      }
      Path resourcesDir = CacheUtils.resourcesDir(baseCacheDir);
      try {
        for (Resource updatedResource : contextDefinition.getResources()) {
          File file = cacheResource(retryFactory, resourcesDir, updatedResource);
          localFiles.add(file.getAbsoluteFile());
          LOG.trace("Added element {} to classpath", file);
        }
      } finally {
        lockInfo.unlock();
      }
    } catch (Exception e) {
      LOG.error("Error initializing context: " + contextDefinition.getContextName(), e);
      throw e;
    }
    return new URLClassLoaderHelper(
        contextDefinition.getContextName() + "_" + contextDefinition.getChecksum(),
        localFiles.stream().map(File::toURI).map(t -> {
          try {
            return t.toURL();
          } catch (MalformedURLException e) {
            // this shouldn't happen since these are local files
            throw new UncheckedIOException(e);
          }
        }).toArray(URL[]::new));
  }

  private static File cacheResource(final RetryFactory retryFactory, final Path contextCacheDir,
      final Resource resource)
      throws InterruptedException, IOException, ContextClassLoaderException, URISyntaxException {
    final FileResolver source = FileResolver.resolve(resource.getLocation());
    final Path tmpCacheLocation =
        contextCacheDir.resolve(source.getFileName() + "_" + resource.getChecksum() + "_tmp");
    final Path finalCacheLocation =
        contextCacheDir.resolve(source.getFileName() + "_" + resource.getChecksum());
    final File cacheFile = finalCacheLocation.toFile();
    if (!Files.exists(finalCacheLocation)) {
      Retry retry = retryFactory.createRetry();
      boolean successful = false;
      while (!successful) {
        LOG.trace("Caching resource {} at {}", source.getURL(), cacheFile.getAbsolutePath());
        try (InputStream is = source.getInputStream()) {
          Files.copy(is, tmpCacheLocation, REPLACE_EXISTING);
          Files.move(tmpCacheLocation, finalCacheLocation, ATOMIC_MOVE);
          successful = true;
          retry.logCompletion(LOG,
              "Resource " + source.getURL() + " cached locally as " + finalCacheLocation);
        } catch (IOException e) {
          LOG.error("Error copying resource from {} to {}. Retrying...", source.getURL(),
              finalCacheLocation, e);
          retry.logRetry(LOG, "Unable to cache resource " + source.getURL());
          retry.waitForNextAttempt(LOG, "Cache resource " + source.getURL());
        } finally {
          retry.useRetry();
        }
      }
      final String checksum = Constants.getChecksummer().digestAsHex(cacheFile);
      if (!resource.getChecksum().equals(checksum)) {
        LOG.error(
            "Checksum {} for resource {} does not match checksum in context definition {}, removing cached copy.",
            checksum, source.getURL(), resource.getChecksum());
        Files.delete(finalCacheLocation);
        throw new IllegalStateException("Checksum " + checksum + " for resource " + source.getURL()
            + " does not match checksum in context definition " + resource.getChecksum());
      }
      return cacheFile;
    } else {
      // File exists, return new ClassPathElement based on existing file
      LOG.trace("Resource {} is already cached at {}", source.getURL(),
          cacheFile.getAbsolutePath());
      return cacheFile;
    }
  }

}
