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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.classloader.lcc.Constants;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Retry.RetryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStore {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStore.class);

  static final Set<PosixFilePermission> CACHE_DIR_PERMS =
      EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE);
  static final FileAttribute<Set<PosixFilePermission>> PERMISSIONS =
      PosixFilePermissions.asFileAttribute(CACHE_DIR_PERMS);

  private final Path baseDir;
  private final Path contextsDir;
  private final Path resourcesDir;

  public LocalStore(final Path baseDir) throws IOException {
    this.baseDir = Objects.requireNonNull(baseDir);
    this.contextsDir = baseDir.resolve("contexts");
    this.resourcesDir = baseDir.resolve("resources");
    Files.createDirectories(contextsDir, PERMISSIONS);
    Files.createDirectories(resourcesDir, PERMISSIONS);
  }

  public Path baseDir() {
    return baseDir;
  }

  public Path contextsDir() {
    return contextsDir;
  }

  public Path resourcesDir() {
    return resourcesDir;
  }

  public URLClassLoaderParams storeContextResources(final ContextDefinition contextDefinition)
      throws IOException, ContextClassLoaderException, InterruptedException, URISyntaxException {
    final RetryFactory retryFactory =
        Retry.builder().infiniteRetries().retryAfter(1, TimeUnit.SECONDS)
            .incrementBy(1, TimeUnit.SECONDS).maxWait(5, TimeUnit.MINUTES).backOffFactor(2)
            .logInterval(1, TimeUnit.SECONDS).createFactory();
    requireNonNull(contextDefinition, "definition must be supplied");
    final Set<File> localFiles = new LinkedHashSet<>();
    Path lockFile;
    LockInfo lockInfo = null;
    try {
      lockFile = contextsDir.resolve("lock_file");
      do {
        try {
          lockInfo = LockInfo.lockFile(lockFile);
        } catch (IOException e) {
          throw new ContextClassLoaderException(
              "Error creating lock file in context cache directory " + lockFile.toAbsolutePath(),
              e);
        }
        if (lockInfo == null) {
          // something else is updating this directory
          LOG.info(
              "Directory {} locked, another process must be updating the class loader contents. "
                  + "Retrying in 1 second",
              contextsDir);
          Thread.sleep(1000);
        }
      } while (lockInfo == null);
      cacheContext(contextDefinition);
      for (Resource updatedResource : contextDefinition.getResources()) {
        File file = cacheResource(retryFactory, updatedResource);
        localFiles.add(file.getAbsoluteFile());
        LOG.trace("Added element {} to classpath", file);
      }
    } catch (Exception e) {
      LOG.error("Error initializing context: " + contextDefinition.getContextName(), e);
      throw e;
    } finally {
      if (lockInfo != null) {
        lockInfo.close();
      }
    }
    return new URLClassLoaderParams(
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

  private void cacheContext(final ContextDefinition contextDefinition) throws IOException {
    Files.write(
        contextsDir.resolve(
            contextDefinition.getContextName() + "_" + contextDefinition.getChecksum() + ".json"),
        contextDefinition.toJson().getBytes(UTF_8));
  }

  private File cacheResource(final RetryFactory retryFactory, final Resource resource)
      throws InterruptedException, IOException, ContextClassLoaderException, URISyntaxException {
    final FileResolver source = FileResolver.resolve(resource.getLocation());
    final Path tmpCacheLocation =
        resourcesDir.resolve(source.getFileName() + "_" + resource.getChecksum() + "_tmp");
    final Path finalCacheLocation =
        resourcesDir.resolve(source.getFileName() + "_" + resource.getChecksum());
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
