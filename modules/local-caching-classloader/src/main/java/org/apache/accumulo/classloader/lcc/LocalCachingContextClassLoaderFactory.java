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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.classloader.lcc.cache.CacheUtils;
import org.apache.accumulo.classloader.lcc.manifest.Manifest;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.classloader.lcc.state.Contexts;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ContextClassLoaderFactory implementation that does the creates and maintains a ClassLoader for
 * a named context. This factory expects the system property {@code Constants#MANIFEST_URL_PROPERTY}
 * to be set to the URL of a json formatted manifest file. The manifest file contains an interval at
 * which this class should monitor the manifest file for changes and a mapping of context names to
 * ContextDefinitions. Each ContextDefinition contains a list of resources. Each resource is defined
 * by a URL to the file and an expected MD5 hash value.
 *
 * The URLs supplied for the manifest file and for the resources can use one of the following
 * protocols: file://, http://, or hdfs://.
 *
 * As this class processes the ContextDefinitions it fetches the contents of the resource from the
 * resource URL and caches it in a directory on the local filesystem. This class uses the value of
 * thesystem property {@code Constants#CACHE_DIR_PROPERTY} as the root directory and creates a
 * subdirectory for each context name. Each context cache directory contains a lock file and a copy
 * of each fetched resource that is named using the following format:
 * fileName_md5Hash.fileNameSuffix.
 *
 * The lock file prevents processes from manipulating the contexts of the context cache directory
 * concurrently, which enables the cache directories to be shared among multiple processes on the
 * host.
 *
 * Note that because the cache directory is shared among multiple processes, and one process can't
 * know what the other processes are doing, this class cannot clean up the shared cache directory.
 * It is left to the user to remove unused context cache directories and unused old files within a
 * context cache directory.
 *
 */
public class LocalCachingContextClassLoaderFactory implements ContextClassLoaderFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalCachingContextClassLoaderFactory.class);

  private final AtomicReference<URL> manifestLocation = new AtomicReference<>();
  private final AtomicReference<Manifest> manifest = new AtomicReference<>();
  private final Contexts contexts = new Contexts(manifest);

  private Manifest parseManifest(URL url) throws ContextClassLoaderException {
    LOG.trace("Retrieving manifest file from {}", url);
    FileResolver resolver = FileResolver.resolve(url);
    try {
      try (InputStream is = resolver.getInputStream()) {
        return Constants.GSON.fromJson(new InputStreamReader(is), Manifest.class);
      }
    } catch (IOException e) {
      throw new ContextClassLoaderException("Error reading manifest file: " + resolver.getURL(), e);
    }
  }

  private URL getAndMonitorManifest() throws ContextClassLoaderException {
    final String manifestPropValue = System.getProperty(Constants.MANIFEST_URL_PROPERTY);
    if (manifestPropValue == null) {
      throw new ContextClassLoaderException(
          "System property " + Constants.MANIFEST_URL_PROPERTY + " not set.");
    }
    try {
      final URL url = new URL(manifestPropValue);
      final Manifest m = parseManifest(url);
      manifest.compareAndSet(null, m);
      contexts.update();
      Constants.EXECUTOR.scheduleWithFixedDelay(() -> {
        try {
          final AtomicBoolean updateRequired = new AtomicBoolean(false);
          final Manifest mUpdate = parseManifest(manifestLocation.get());
          manifest.getAndAccumulate(mUpdate, (curr, update) -> {
            try {
              // If the Manifest file has not changed, then continue to use
              // the current Manifest. If it has changed, then update the
              // Contexts and use the new one.
              if (Arrays.equals(curr.getChecksum(), update.getChecksum())) {
                LOG.trace("Manifest file has not changed");
                return curr;
              } else {
                LOG.debug("Manifest file has changed, updating contexts");
                updateRequired.set(true);
                return update;
              }
            } catch (NoSuchAlgorithmException e) {
              LOG.error(
                  "Error computing checksum during manifest update, retaining current manifest", e);
              return curr;
            }
          });
          if (updateRequired.get()) {
            contexts.update();
          }
        } catch (Exception e) {
          LOG.error("Error parsing manifest at {}", url);
        }
      }, m.getMonitorIntervalSeconds(), m.getMonitorIntervalSeconds(), TimeUnit.SECONDS);
      LOG.debug("Monitoring manifest file {} for changes at {} second intervals", url,
          m.getMonitorIntervalSeconds());
      return url;
    } catch (IOException e) {
      throw new ContextClassLoaderException(
          "Error parsing manifest at " + Constants.MANIFEST_URL_PROPERTY, e);
    }
  }

  @Override
  public ClassLoader getClassLoader(String contextName) throws ContextClassLoaderException {

    // If the location is not already set, get the Manifest location,
    // parse it, and start a thread to monitor it for updates.
    if (manifestLocation.get() == null) {
      CacheUtils.createBaseCacheDir();
      manifestLocation.compareAndSet(null, getAndMonitorManifest());
    }

    return contexts.getContextClassLoader(contextName);
  }

}
