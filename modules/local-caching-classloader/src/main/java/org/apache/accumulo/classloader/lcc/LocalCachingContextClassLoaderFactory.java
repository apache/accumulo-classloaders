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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ref.SoftReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.classloader.lcc.cache.CacheUtils;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ContextClassLoaderFactory implementation that creates and maintains a ClassLoader for a named
 * context. This factory expects the parameter passed to {@link #getClassLoader(String)} to be the
 * URL of a json formatted {@link ContextDefinition} file. The file contains an interval at which
 * this class should monitor the file for changes and a list of {@link Resource} objects. Each
 * resource is defined by a URL to the file and an expected MD5 hash value.
 * <p>
 * The URLs supplied for the context definition file and for the resources can use one of the
 * following protocols: file://, http://, or hdfs://.
 * <p>
 * As this class processes the ContextDefinition it fetches the contents of the resource from the
 * resource URL and caches it in a directory on the local filesystem. This class uses the value of
 * the system property {@link Constants#CACHE_DIR_PROPERTY} as the root directory and creates a
 * sub-directory for each context name. Each context cache directory contains a lock file and a copy
 * of each fetched resource that is named using the following format: fileName_checksum.
 * <p>
 * The lock file prevents processes from manipulating the contexts of the context cache directory
 * concurrently, which enables the cache directories to be shared among multiple processes on the
 * host.
 * <p>
 * Note that because the cache directory is shared among multiple processes, and one process can't
 * know what the other processes are doing, this class cannot clean up the shared cache directory.
 * It is left to the user to remove unused context cache directories and unused old files within a
 * context cache directory.
 */
public class LocalCachingContextClassLoaderFactory implements ContextClassLoaderFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalCachingContextClassLoaderFactory.class);

  private final ConcurrentHashMap<String,SoftReference<LocalCachingContextClassLoader>> contexts =
      new ConcurrentHashMap<>();

  private ContextDefinition parseContextDefinition(URL url) throws ContextClassLoaderException {
    LOG.trace("Retrieving context definition file from {}", url);
    FileResolver resolver = FileResolver.resolve(url);
    try {
      try (InputStream is = resolver.getInputStream()) {
        ContextDefinition def =
            Constants.GSON.fromJson(new InputStreamReader(is, UTF_8), ContextDefinition.class);
        if (def == null) {
          throw new ContextClassLoaderException(
              "ContextDefinition null for context definition file: " + resolver.getURL());
        }
        return def;
      }
    } catch (IOException e) {
      throw new ContextClassLoaderException(
          "Error reading context definition file: " + resolver.getURL(), e);
    }
  }

  private void monitorContext(final String contextLocation, boolean initialCall, int interval) {
    final SoftReference<LocalCachingContextClassLoader> ccl = contexts.get(contextLocation);
    if (!initialCall && ccl == null) {
      // context has been removed from the map, no need to check for update
      return;
    }
    if (!initialCall && ccl.get() == null) {
      // classloader has been garbage collected. Remove from the map and return
      contexts.remove(contextLocation);
      return;
    }
    Constants.EXECUTOR.schedule(() -> {
      final LocalCachingContextClassLoader classLoader = contexts.get(contextLocation).get();
      final ContextDefinition currentDef = classLoader.getDefinition();
      try {
        final URL contextManifest = new URL(contextLocation);
        final ContextDefinition update = parseContextDefinition(contextManifest);
        if (!Arrays.equals(currentDef.getChecksum(), update.getChecksum())) {
          LOG.debug("Context definition for {} has changed", currentDef.getContextName());
          classLoader.update(update);
        } else {
          LOG.debug("Context definition for {} has not changed", currentDef.getContextName());
        }
        monitorContext(contextLocation, false, update.getMonitorIntervalSeconds());
      } catch (Exception e) {
        LOG.error("Error parsing updated context definition at {}. Classloader NOT updated!",
            contextLocation, e);
      }
    }, interval, TimeUnit.SECONDS);
    LOG.trace("Monitoring context definition file {} for changes at {} second intervals",
        contextLocation, interval);
  }

  @Override
  public ClassLoader getClassLoader(final String contextLocation)
      throws ContextClassLoaderException {
    try {
      SoftReference<LocalCachingContextClassLoader> ccl =
          contexts.computeIfAbsent(contextLocation, cn -> {
            return new SoftReference<>(createContextClassLoader(contextLocation));
          });
      final LocalCachingContextClassLoader lcccl = ccl.get();
      if (ccl.get() == null) {
        // SoftReference is in the map, but the classloader has been garbage collected.
        // Need to recreate it
        contexts.remove(contextLocation);
        return getClassLoader(contextLocation);
      }
      return lcccl.getClassloader();
    } catch (RuntimeException re) {
      Throwable t = re.getCause();
      if (t != null && t instanceof ContextClassLoaderException) {
        throw (ContextClassLoaderException) t;
      } else {
        throw new ContextClassLoaderException(re.getMessage(), re);
      }
    }
  }

  private LocalCachingContextClassLoader createContextClassLoader(final String contextLocation) {
    try {
      URL contextManifest = new URL(contextLocation);
      CacheUtils.createBaseCacheDir();
      ContextDefinition m = parseContextDefinition(contextManifest);
      LocalCachingContextClassLoader newCcl = new LocalCachingContextClassLoader(m);
      newCcl.initialize();
      monitorContext(contextLocation, true, m.getMonitorIntervalSeconds());
      return newCcl;
    } catch (MalformedURLException e) {
      throw new RuntimeException(
          "Expected valid URL to context definition file but received: " + contextLocation, e);
    } catch (ContextClassLoaderException e) {
      throw new RuntimeException("Error processing context definition", e);
    }
  }

}
