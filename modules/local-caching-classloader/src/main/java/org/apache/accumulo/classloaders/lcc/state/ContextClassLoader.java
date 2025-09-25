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
package org.apache.accumulo.classloaders.lcc.state;

import java.io.File;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.classloader.lcc.Constants;
import org.apache.accumulo.classloader.lcc.cache.CacheUtils;
import org.apache.accumulo.classloader.lcc.manifest.ContextDefinition;
import org.apache.accumulo.classloader.lcc.manifest.Manifest;
import org.apache.accumulo.classloader.lcc.manifest.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextClassLoader {

  public static class ClassPathElement {
    private final FileResolver remote;
    private final URL localCachedCopy;
    private final String localCachedCopyDigest;

    public ClassPathElement(FileResolver remote, URL localCachedCopy,
        String localCachedCopyDigest) {
      super();
      this.remote = remote;
      this.localCachedCopy = localCachedCopy;
      this.localCachedCopyDigest = localCachedCopyDigest;
    }

    public FileResolver getRemote() {
      return remote;
    }

    public URL getLocalCachedCopy() {
      return localCachedCopy;
    }

    public String getLocalCachedCopyDigest() {
      return localCachedCopyDigest;
    }
  }

  private ClassPathElement cacheResource(final Resource resource) throws Exception {

    final DigestUtils digest = new DigestUtils("MD5");
    final FileResolver source = FileResolver.resolve(resource.getURL());
    final Path cacheLocation =
        contextCacheDir.resolve(source.getFileName() + "_" + resource.getChecksum());
    final File cacheFile = cacheLocation.toFile();
    if (!Files.exists(cacheLocation)) {
      Files.copy(source.getInputStream(), cacheLocation);
      String md5 = digest.digestAsHex(cacheFile);
      if (!resource.getChecksum().equals(digest)) {
        // What we just wrote does not match the Manifest.
      }
      return new ClassPathElement(source, cacheFile.toURI().toURL(), md5);
    } else {
      // File exists, return new ClassPathElement based on existing file
      String fileName = cacheFile.getName();
      String[] parts = fileName.split("_");
      return new ClassPathElement(source, cacheFile.toURI().toURL(), parts[1]);
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(ContextClassLoader.class);

  private final AtomicReference<Manifest> manifest;
  private final Path contextCacheDir;
  private final String contextName;
  private final Set<ClassPathElement> elements = new HashSet<>();
  private final AtomicBoolean elementsChanged = new AtomicBoolean(true);
  private volatile ContextDefinition definition;
  private volatile WeakReference<URLClassLoader> classloader = null;
  private volatile ScheduledFuture<?> refreshTask;

  public ContextClassLoader(AtomicReference<Manifest> manifest, String name)
      throws ContextClassLoaderException {
    this.manifest = manifest;
    this.contextName = name;
    this.definition = manifest.get().getContexts().get(contextName);
    this.contextCacheDir = CacheUtils.createOrGetContextCacheDir(contextName);
  }

  public ContextDefinition getDefinition() {
    return definition;
  }

  private void scheduleRefresh() {
    // Schedule a one-shot task in the event the monitor interval changes
    this.refreshTask = Constants.EXECUTOR.schedule(() -> update(),
        this.definition.getContextMonitorIntervalSeconds(), TimeUnit.SECONDS);
  }

  private void update() {
    ContextDefinition update = manifest.get().getContexts().get(contextName);
    if (update != null) {
      updateDefinition(update);
    }
  }

  public void initialize() {
    try {
      synchronized (elements) {
        final Pair<FileChannel,FileLock> lockPair = CacheUtils.lockContextCacheDir(contextCacheDir);
        if (lockPair == null) {
          // something else is updating this directory
          return;
        }
        try {
          for (Resource r : definition.getResources()) {
            ClassPathElement cpe = cacheResource(r);
            addElement(cpe);
          }
          scheduleRefresh();
        } finally {
          lockPair.getSecond().release();
          lockPair.getFirst().close();
        }
      }
    } catch (Exception e) {
      LOG.error("Error initializing context: " + contextName, e);
    }
  }

  private void updateDefinition(ContextDefinition update) {
    synchronized (elements) {
      try {
        final Pair<FileChannel,FileLock> lockPair = CacheUtils.lockContextCacheDir(contextCacheDir);
        if (lockPair == null) {
          // something else is updating this directory
          return;
        }
        try {
          if (!definition.getResources().equals(update.getResources())) {

            for (Resource updatedResource : update.getResources()) {

              ClassPathElement existing = findElementBySourceLocation(updatedResource.getURL());
              if (existing == null) {
                // new resource
                ClassPathElement cpe = cacheResource(updatedResource);
                addElement(cpe);
              } else if (existing.getLocalCachedCopyDigest()
                  .equals(updatedResource.getChecksum())) {
                removeElement(existing);
                ClassPathElement cpe = cacheResource(updatedResource);
                addElement(cpe);
              }
            }
          }
          this.definition = update;
          scheduleRefresh();
        } finally {
          lockPair.getSecond().release();
          lockPair.getFirst().close();
        }
      } catch (Exception e) {
        LOG.error("Error updating context: " + contextName, e);
      }
    }
  }

  private ClassPathElement findElementBySourceLocation(URL source) {
    for (ClassPathElement cpe : elements) {
      if (cpe.getRemote().getURL().equals(source)) {
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

  public void clear() {
    synchronized (elements) {
      refreshTask.cancel(true);
      elements.clear();
      elementsChanged.set(false);
      if (classloader != null) {
        classloader.clear();
      }
    }
  }

  public ClassLoader getClassloader() {
    synchronized (elements) {
      if (classloader == null || elementsChanged.get()) {
        URL[] urls = new URL[elements.size()];
        Iterator<ClassPathElement> iter = elements.iterator();
        for (int x = 0; x < elements.size(); x++) {
          urls[x] = iter.next().getLocalCachedCopy();
        }
        elementsChanged.set(false);
        classloader = new WeakReference<>(
            new URLClassLoader(contextName, urls, this.getClass().getClassLoader()));
      }
    }
    return classloader.get();
  }
}
