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
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.classloader.lcc.util.DeduplicationCache;
import org.apache.accumulo.classloader.lcc.util.LocalStore;
import org.apache.accumulo.classloader.lcc.util.URLClassLoaderParams;
import org.apache.accumulo.classloader.lcc.util.WrappedException;
import org.apache.accumulo.core.spi.common.ContextClassLoaderEnvironment;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.core.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
 * the property {@link Constants#CACHE_DIR_PROPERTY} passed via
 * {@link #init(ContextClassLoaderEnvironment)} as the root directory and creates a sub-directory
 * for each context name. Each context cache directory contains a lock file and a copy of each
 * fetched resource that is named using the following format: fileName_checksum.
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

  // stores the latest seen ContextDefinition for a context name
  private final ConcurrentHashMap<String,ContextDefinition> contextDefs = new ConcurrentHashMap<>();

  // to keep this coherent with the contextDefs, updates to this should be done in the compute
  // method of contextDefs
  private final DeduplicationCache<String,URLClassLoaderParams,
      URLClassLoader> classloaders = new DeduplicationCache<>(
          (String key, URLClassLoaderParams helper) -> helper.createClassLoader(),
          Duration.ofHours(24));

  private final Map<String,Timer> classloaderFailures = new HashMap<>();
  private final AtomicReference<LocalStore> localStore = new AtomicReference<>();
  private volatile Duration updateFailureGracePeriodMins;

  private ContextDefinition parseContextDefinition(final String contextLocation)
      throws ContextClassLoaderException {
    URL url;
    try {
      url = new URL(contextLocation);
    } catch (MalformedURLException e) {
      throw new ContextClassLoaderException(
          "Expected valid URL to context definition file but received: " + contextLocation, e);
    }
    LOG.trace("Retrieving context definition file from {}", contextLocation);
    final FileResolver resolver = FileResolver.resolve(url);
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

  /**
   * Schedule a task to execute at {@code interval} seconds to update the LocalCachingContext if the
   * ContextDefinition has changed. The task schedules a follow-on task at the update interval value
   * (if it changed).
   */
  private void monitorContext(final String contextLocation, int interval) {
    final Runnable updateTask = () -> {
      ContextDefinition currentDef = contextDefs.compute(contextLocation, (k, v) -> {
        if (v == null) {
          return null;
        }
        if (!classloaders
            .anyMatch(k2 -> k2.substring(0, k2.lastIndexOf('-')).equals(v.getContextName()))) {
          // context has been removed from the map, no need to check for update
          LOG.debug("ClassLoader for context {} not present, no longer monitoring for changes",
              contextLocation);
          return null;
        }
        return v;
      });
      if (currentDef == null) {
        // context has been removed from the map, no need to check for update
        LOG.debug("ContextDefinition for context {} not present, no longer monitoring for changes",
            contextLocation);
        return;
      }
      int nextInterval = interval;
      try {
        final ContextDefinition update = parseContextDefinition(contextLocation);
        if (!currentDef.getChecksum().equals(update.getChecksum())) {
          LOG.debug("Context definition for {} has changed", contextLocation);
          if (!currentDef.getContextName().equals(update.getContextName())) {
            LOG.warn(
                "Context name changed for context {}, but context cache directory will remain {} (old={}, new={})",
                contextLocation, currentDef.getContextName(), currentDef.getContextName(),
                update.getContextName());
          }
          localStore.get().storeContextResources(update);
          contextDefs.put(contextLocation, update);
          nextInterval = update.getMonitorIntervalSeconds();
          classloaderFailures.remove(contextLocation);
        } else {
          LOG.trace("Context definition for {} has not changed", contextLocation);
        }
      } catch (ContextClassLoaderException | InterruptedException | IOException | URISyntaxException
          | RuntimeException e) {
        LOG.error("Error parsing updated context definition at {}. Classloader NOT updated!",
            contextLocation, e);
        final Timer failureTimer = classloaderFailures.get(contextLocation);
        if (updateFailureGracePeriodMins.isZero()) {
          // failure monitoring is disabled
          LOG.debug("Property {} not set, not tracking classloader failures for context {}",
              Constants.UPDATE_FAILURE_GRACE_PERIOD_MINS, contextLocation);
        } else if (failureTimer == null) {
          // first failure, start the timer
          classloaderFailures.put(contextLocation, Timer.startNew());
          LOG.debug(
              "Tracking classloader failures for context {}, will NOT return working classloader if failures continue for {} minutes",
              contextLocation, updateFailureGracePeriodMins.toMinutes());
        } else if (failureTimer.hasElapsed(updateFailureGracePeriodMins)) {
          // has been failing for the grace period
          // unset the classloader reference so that the failure
          // will return from getClassLoader in the calling thread
          LOG.info("Grace period for failing classloader has elapsed for context {}",
              contextLocation);
          contextDefs.remove(contextLocation);
          classloaderFailures.remove(contextLocation);
        } else {
          LOG.trace("Failing to update classloader for context {} within the grace period",
              contextLocation, e);
        }
      } finally {
        monitorContext(contextLocation, nextInterval);
      }
    };
    Constants.EXECUTOR.schedule(updateTask, interval, TimeUnit.SECONDS);
    LOG.trace("Monitoring context definition file {} for changes at {} second intervals",
        contextLocation, interval);
  }

  // for tests only
  void resetForTests() {
    // Removing the contexts will cause the
    // background monitor task to end
    contextDefs.clear();
  }

  @Override
  public void init(ContextClassLoaderEnvironment env) {
    String value = requireNonNull(env.getConfiguration().get(Constants.CACHE_DIR_PROPERTY),
        "Property " + Constants.CACHE_DIR_PROPERTY + " not set, cannot create cache directory.");
    String graceProp = env.getConfiguration().get(Constants.UPDATE_FAILURE_GRACE_PERIOD_MINS);
    long graceMins = graceProp == null ? 0 : Long.parseLong(graceProp);
    updateFailureGracePeriodMins = Duration.ofMinutes(graceMins);
    final Path baseCacheDir;
    try {
      if (value.startsWith("file:")) {
        baseCacheDir = Path.of(new URL(value).toURI());
      } else if (value.startsWith("/")) {
        baseCacheDir = Path.of(value);
      } else {
        throw new ContextClassLoaderException(
            "Base directory is neither a file URL nor an absolute file path");
      }
      localStore.set(new LocalStore(baseCacheDir));
    } catch (IOException | URISyntaxException | ContextClassLoaderException e) {
      throw new IllegalStateException("Error creating base cache directories at " + value, e);
    }
  }

  @Override
  public ClassLoader getClassLoader(final String contextLocation)
      throws ContextClassLoaderException {
    Preconditions.checkState(localStore.get() != null,
        "init not called before calling getClassLoader");
    requireNonNull(contextLocation, "context name must be supplied");
    final AtomicBoolean newlyCreated = new AtomicBoolean(false);
    final AtomicReference<URLClassLoader> cl = new AtomicReference<>();
    ContextDefinition def;
    try {
      def = contextDefs.compute(contextLocation, (k, v) -> {
        ContextDefinition def2;
        if (v == null) {
          newlyCreated.set(true);
          try {
            def2 = parseContextDefinition(k);
          } catch (ContextClassLoaderException e) {
            throw new WrappedException(e);
          }
        } else {
          def2 = v;
        }
        final URLClassLoader cl2 =
            classloaders.computeIfAbsent(def2.getContextName() + "-" + def2.getChecksum(),
                (Supplier<URLClassLoaderParams>) () -> {
                  try {
                    return localStore.get().storeContextResources(def2);
                  } catch (Exception e) {
                    throw new WrappedException(e);
                  }
                });
        cl.set(cl2);
        return def2;
      });
    } catch (WrappedException e) {
      Throwable t = e.getCause();
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      } else if (t instanceof ContextClassLoaderException) {
        throw (ContextClassLoaderException) t;
      }
      throw new ContextClassLoaderException(t.getMessage(), t);
    } catch (RuntimeException e) {
      throw new ContextClassLoaderException(e.getMessage(), e);
    }
    if (newlyCreated.get()) {
      monitorContext(contextLocation, def.getMonitorIntervalSeconds());
    }
    return cl.get();
  }

}
