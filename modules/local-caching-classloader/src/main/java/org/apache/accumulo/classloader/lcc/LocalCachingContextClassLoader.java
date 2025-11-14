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

  public static class ClassPathElement {
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

    public FileResolver getResolver() {
      return resolver;
    }

    public URL getLocalCachedCopyLocation() {
      return localCachedCopyLocation;
    }

    public String getLocalCachedCopyDigest() {
      return localCachedCopyDigest;
    }
  }

  private ClassPathElement cacheResource(final Resource resource) throws Exception {

    final FileResolver source = FileResolver.resolve(resource.getURL());
    final Path cacheLocation =
        contextCacheDir.resolve(source.getFileName() + "_" + resource.getChecksum());
    final File cacheFile = cacheLocation.toFile();
    if (!Files.exists(cacheLocation)) {
      try (InputStream is = source.getInputStream()) {
        Files.copy(is, cacheLocation);
      }
      final String checksum = Constants.getChecksummer().digestAsHex(cacheFile);
      if (!resource.getChecksum().equals(checksum)) {
        // TODO: What we just wrote does not match the MD5 in the Resource description.
      }
      return new ClassPathElement(source, cacheFile.toURI().toURL(), checksum);
    } else {
      // File exists, return new ClassPathElement based on existing file
      String fileName = cacheFile.getName();
      String[] parts = fileName.split("_");
      return new ClassPathElement(source, cacheFile.toURI().toURL(), parts[1]);
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

  public ContextDefinition getDefinition() {
    return definition.get();
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
          for (Resource updatedResource : update.getResources()) {
            ClassPathElement existing = findElementBySourceLocation(updatedResource.getURL());
            if (existing == null) {
              // new resource
              ClassPathElement cpe = cacheResource(updatedResource);
              addElement(cpe);
            } else if (existing.getLocalCachedCopyDigest().equals(updatedResource.getChecksum())) {
              removeElement(existing);
              ClassPathElement cpe = cacheResource(updatedResource);
              addElement(cpe);
            }
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

  private ClassPathElement findElementBySourceLocation(URL source) {
    for (ClassPathElement cpe : elements) {
      if (cpe.getResolver().getURL().equals(source)) {
        return cpe;
      }
    }
    return null;
  }

  private void removeElement(ClassPathElement element) {
    synchronized (elements) {
      elements.remove(element);
      elementsChanged.set(true);
    }
  }

  private void addElement(ClassPathElement element) {
    synchronized (elements) {
      elements.add(element);
      elementsChanged.set(true);
    }
  }

  public ClassLoader getClassloader() {
    synchronized (elements) {
      if (classloader.get() == null || elementsChanged.get()) {
        URL[] urls = new URL[elements.size()];
        Iterator<ClassPathElement> iter = elements.iterator();
        for (int x = 0; x < elements.size(); x++) {
          urls[x] = iter.next().getLocalCachedCopyLocation();
        }
        elementsChanged.set(false);
        classloader.set(new URLClassLoader(contextName, urls, this.getClass().getClassLoader()));
      }
    }
    return classloader.get();
  }
}
