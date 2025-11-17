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

import java.io.File;
import java.io.InputStream;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.classloader.lcc.cache.CacheUtils;
import org.apache.accumulo.classloader.lcc.cache.CacheUtils.LockInfo;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalCachingContextClassLoader {

  private static class ClassPathElement {
    private final FileResolver resolver;
    private final URL localCachedCopyLocation;
    private final String localCachedCopyDigest;

    public ClassPathElement(FileResolver resolver, URL localCachedCopy,
        String localCachedCopyDigest) {
      this.resolver = Objects.requireNonNull(resolver, "resolver must be supplied");
      this.localCachedCopyLocation =
          Objects.requireNonNull(localCachedCopy, "local cached copy location must be supplied");
      this.localCachedCopyDigest =
          Objects.requireNonNull(localCachedCopyDigest, "local cached copy md5 must be supplied");
    }

    @SuppressWarnings("unused")
    public FileResolver getResolver() {
      return resolver;
    }

    public URL getLocalCachedCopyLocation() {
      return localCachedCopyLocation;
    }

    @SuppressWarnings("unused")
    public String getLocalCachedCopyDigest() {
      return localCachedCopyDigest;
    }

    @Override
    public int hashCode() {
      return Objects.hash(localCachedCopyDigest, localCachedCopyLocation, resolver);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
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

  private static final Logger LOG = LoggerFactory.getLogger(LocalCachingContextClassLoader.class);

  private final Path contextCacheDir;
  private final String contextName;
  private final Set<ClassPathElement> elements = new HashSet<>();
  private final AtomicBoolean elementsChanged = new AtomicBoolean(true);
  private final AtomicReference<URLClassLoader> classloader = new AtomicReference<>();
  private final AtomicReference<ContextDefinition> definition = new AtomicReference<>();

  public LocalCachingContextClassLoader(ContextDefinition contextDefinition)
      throws ContextClassLoaderException {
    this.definition.set(Objects.requireNonNull(contextDefinition, "definition must be supplied"));
    this.contextName = this.definition.get().getContextName();
    this.contextCacheDir = CacheUtils.createOrGetContextCacheDir(contextName);
  }

  @Deprecated
  @Override
  protected final void finalize() {
    /*
     * unused; this is final due to finalizer attacks since the constructor throws exceptions (see
     * spotbugs CT_CONSTRUCTOR_THROW)
     */
  }

  public ContextDefinition getDefinition() {
    return definition.get();
  }

  private ClassPathElement cacheResource(final Resource resource) throws Exception {

    final FileResolver source = FileResolver.resolve(resource.getURL());
    final Path cacheLocation =
        contextCacheDir.resolve(source.getFileName() + "_" + resource.getChecksum());
    final File cacheFile = cacheLocation.toFile();
    if (!Files.exists(cacheLocation)) {
      LOG.trace("Caching resource {} at {}", source.getURL(), cacheFile.getAbsolutePath());
      try (InputStream is = source.getInputStream()) {
        Files.copy(is, cacheLocation);
      }
      final String checksum = Constants.getChecksummer().digestAsHex(cacheFile);
      if (!resource.getChecksum().equals(checksum)) {
        LOG.error("Checksum {} for resource {} does not match checksum in context definition {}",
            checksum, source.getURL(), resource.getChecksum());
        throw new IllegalStateException("Checksum " + checksum + " for resource " + source.getURL()
            + " does not match checksum in context definition " + resource.getChecksum());
      }
      return new ClassPathElement(source, cacheFile.toURI().toURL(), checksum);
    } else {
      // File exists, return new ClassPathElement based on existing file
      LOG.trace("Resource {} is already cached at {}", source.getURL(),
          cacheFile.getAbsolutePath());
      String fileName = cacheFile.getName();
      String[] parts = fileName.split("_");
      return new ClassPathElement(source, cacheFile.toURI().toURL(), parts[1]);
    }
  }

  public void initialize() {
    try {
      synchronized (elements) {
        final LockInfo lockInfo = CacheUtils.lockContextCacheDir(contextCacheDir);
        if (lockInfo == null) {
          // something else is updating this directory
          return;
        }
        try {
          for (Resource r : definition.get().getResources()) {
            ClassPathElement cpe = cacheResource(r);
            addElement(cpe);
          }
        } finally {
          lockInfo.unlock();
        }
      }
    } catch (Exception e) {
      LOG.error("Error initializing context: " + contextName, e);
    }
  }

  public void update(final ContextDefinition update) {
    Objects.requireNonNull(update, "definition must be supplied");
    if (definition.get().getResources().equals(update.getResources())) {
      return;
    }
    synchronized (elements) {
      try {
        final LockInfo lockInfo = CacheUtils.lockContextCacheDir(contextCacheDir);
        if (lockInfo == null) {
          // something else is updating this directory
          return;
        }
        try {
          elements.clear();
          for (Resource updatedResource : update.getResources()) {
            ClassPathElement cpe = cacheResource(updatedResource);
            addElement(cpe);
          }
          this.definition.set(update);
        } finally {
          lockInfo.unlock();
        }
      } catch (Exception e) {
        LOG.error("Error updating context: " + contextName, e);
      }
    }
  }

  private void addElement(ClassPathElement element) {
    synchronized (elements) {
      elements.add(element);
      elementsChanged.set(true);
      LOG.trace("Added element {} to classpath", element);
    }
  }

  public ClassLoader getClassloader() {
    synchronized (elements) {
      if (classloader.get() == null || elementsChanged.get()) {
        LOG.trace("Class path contents have changed, creating new classloader");
        URL[] urls = new URL[elements.size()];
        Iterator<ClassPathElement> iter = elements.iterator();
        for (int x = 0; x < elements.size(); x++) {
          urls[x] = iter.next().getLocalCachedCopyLocation();
        }
        elementsChanged.set(false);
        final URLClassLoader cl =
            AccessController.doPrivileged((PrivilegedAction<URLClassLoader>) () -> {
              return new URLClassLoader(contextName, urls, this.getClass().getClassLoader());
            });
        classloader.set(cl);
        LOG.trace("New classloader created from URLs: {}",
            Arrays.asList(classloader.get().getURLs()));
      }
    }
    return classloader.get();
  }
}
