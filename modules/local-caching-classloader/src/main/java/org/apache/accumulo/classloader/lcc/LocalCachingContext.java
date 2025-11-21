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
package org.apache.accumulo.classloader.lcc;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.classloader.lcc.cache.CacheUtils;
import org.apache.accumulo.classloader.lcc.cache.CacheUtils.LockInfo;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Retry.RetryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LocalCachingContext {

  private static class ClassPathElement {
    private final FileResolver resolver;
    private final URL localCachedCopyLocation;
    private final String localCachedCopyDigest;

    public ClassPathElement(FileResolver resolver, URL localCachedCopy,
        String localCachedCopyDigest) {
      this.resolver = requireNonNull(resolver, "resolver must be supplied");
      this.localCachedCopyLocation =
          requireNonNull(localCachedCopy, "local cached copy location must be supplied");
      this.localCachedCopyDigest =
          requireNonNull(localCachedCopyDigest, "local cached copy md5 must be supplied");
    }

    public URL getLocalCachedCopyLocation() {
      return localCachedCopyLocation;
    }

    @Override
    public int hashCode() {
      return hash(localCachedCopyDigest, localCachedCopyLocation, resolver);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ClassPathElement other = (ClassPathElement) obj;
      return Objects.equals(localCachedCopyDigest, other.localCachedCopyDigest)
          && Objects.equals(localCachedCopyLocation, other.localCachedCopyLocation)
          && Objects.equals(resolver, other.resolver);
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append("source: ").append(resolver.getURL());
      buf.append(", cached copy:").append(localCachedCopyLocation);
      return buf.toString();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(LocalCachingContext.class);

  private final Path contextCacheDir;
  private final String contextName;
  private final Set<ClassPathElement> elements = new HashSet<>();
  private final AtomicReference<URLClassLoader> classloader = new AtomicReference<>();
  private final AtomicReference<ContextDefinition> definition = new AtomicReference<>();
  private final RetryFactory retryFactory = Retry.builder().infiniteRetries()
      .retryAfter(1, TimeUnit.SECONDS).incrementBy(1, TimeUnit.SECONDS).maxWait(5, TimeUnit.MINUTES)
      .backOffFactor(2).logInterval(1, TimeUnit.SECONDS).createFactory();

  public LocalCachingContext(ContextDefinition contextDefinition)
      throws IOException, ContextClassLoaderException {
    this.definition.set(requireNonNull(contextDefinition, "definition must be supplied"));
    this.contextName = this.definition.get().getContextName();
    this.contextCacheDir = CacheUtils.createOrGetContextCacheDir(contextName);
  }

  public ContextDefinition getDefinition() {
    return definition.get();
  }

  private ClassPathElement cacheResource(final Resource resource)
      throws InterruptedException, IOException, ContextClassLoaderException, URISyntaxException {
    final FileResolver source = FileResolver.resolve(resource.getURL());
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
      return new ClassPathElement(source, cacheFile.toURI().toURL(), checksum);
    } else {
      // File exists, return new ClassPathElement based on existing file
      LOG.trace("Resource {} is already cached at {}", source.getURL(),
          cacheFile.getAbsolutePath());
      return new ClassPathElement(source, cacheFile.toURI().toURL(), resource.getChecksum());
    }
  }

  private void cacheResources(final ContextDefinition def)
      throws InterruptedException, IOException, ContextClassLoaderException, URISyntaxException {
    synchronized (elements) {
      for (Resource updatedResource : def.getResources()) {
        ClassPathElement cpe = cacheResource(updatedResource);
        elements.add(cpe);
        LOG.trace("Added element {} to classpath", cpe);
      }
      classloader.set(null);
    }
  }

  public void initialize()
      throws InterruptedException, IOException, ContextClassLoaderException, URISyntaxException {
    try {
      LockInfo lockInfo = CacheUtils.lockContextCacheDir(contextCacheDir);
      while (lockInfo == null) {
        // something else is updating this directory
        LOG.info("Directory {} locked, another process must be updating the class loader contents. "
            + "Retrying in 1 second", contextCacheDir);
        Thread.sleep(1000);
        lockInfo = CacheUtils.lockContextCacheDir(contextCacheDir);
      }
      synchronized (elements) {
        try {
          cacheResources(definition.get());
        } finally {
          lockInfo.unlock();
        }
      }
    } catch (Exception e) {
      LOG.error("Error initializing context: " + contextName, e);
      throw e;
    }
  }

  public void update(final ContextDefinition update)
      throws InterruptedException, IOException, ContextClassLoaderException, URISyntaxException {
    requireNonNull(update, "definition must be supplied");
    if (definition.get().getResources().equals(update.getResources())) {
      return;
    }
    try {
      LockInfo lockInfo = CacheUtils.lockContextCacheDir(contextCacheDir);
      while (lockInfo == null) {
        // something else is updating this directory
        LOG.info("Directory {} locked, another process must be updating the class loader contents. "
            + "Retrying in 1 second", contextCacheDir);
        Thread.sleep(1000);
        lockInfo = CacheUtils.lockContextCacheDir(contextCacheDir);
      }
      synchronized (elements) {
        try {
          elements.clear();
          cacheResources(update);
          this.definition.set(update);
        } finally {
          lockInfo.unlock();
        }
      }
    } catch (Exception e) {
      LOG.error("Error updating context: " + contextName, e);
      throw e;
    }
  }

  public ClassLoader getClassloader() {

    ClassLoader currentCL = classloader.get();
    if (currentCL != null) {
      return currentCL;
    }

    synchronized (elements) {

      currentCL = classloader.get();
      if (currentCL != null) {
        return currentCL;
      }

      LOG.trace("Class path contents have changed, creating new classloader");
      URL[] urls = new URL[elements.size()];
      Iterator<ClassPathElement> iter = elements.iterator();
      for (int x = 0; x < elements.size(); x++) {
        urls[x] = iter.next().getLocalCachedCopyLocation();
      }
      final URLClassLoader cl =
          AccessController.doPrivileged((PrivilegedAction<URLClassLoader>) () -> {
            return new URLClassLoader(contextName, urls, this.getClass().getClassLoader());
          });
      classloader.set(cl);
      LOG.trace("New classloader created from URLs: {}",
          Arrays.asList(classloader.get().getURLs()));
      return cl;
    }
  }
}
