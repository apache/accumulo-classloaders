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
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.classloader.lcc.cache.CacheUtils;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.core.spi.common.ContextClassLoaderEnvironment;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.core.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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

  private final Cache<String,LocalCachingContext> contexts =
      Caffeine.newBuilder().expireAfterAccess(24, TimeUnit.HOURS).build();

  private final Map<String,Timer> classloaderFailures = new HashMap<>();
  private volatile String baseCacheDir;
  private volatile Duration updateFailureGracePeriodMins;

  private ContextDefinition parseContextDefinition(final URL url)
      throws ContextClassLoaderException {
    LOG.trace("Retrieving context definition file from {}", url);
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
    Constants.EXECUTOR.schedule(() -> {
      final LocalCachingContext classLoader =
          contexts.policy().getIfPresentQuietly(contextLocation);
      if (classLoader == null) {
        // context has been removed from the map, no need to check for update
        LOG.debug("ClassLoader for context {} not present, no longer monitoring for changes",
            contextLocation);
        return;
      }
      int nextInterval = interval;
      final ContextDefinition currentDef = classLoader.getDefinition();
      try {
        final URL contextLocationUrl = new URL(contextLocation);
        final ContextDefinition update = parseContextDefinition(contextLocationUrl);
        if (!Arrays.equals(currentDef.getChecksum(), update.getChecksum())) {
          LOG.debug("Context definition for {} has changed", contextLocation);
          if (!currentDef.getContextName().equals(update.getContextName())) {
            LOG.warn(
                "Context name changed for context {}, but context cache directory will remain {} (old={}, new={})",
                contextLocation, currentDef.getContextName(), currentDef.getContextName(),
                update.getContextName());
          }
          classLoader.update(update);
          nextInterval = update.getMonitorIntervalSeconds();
          classloaderFailures.remove(contextLocation);
        } else {
          LOG.trace("Context definition for {} has not changed", contextLocation);
        }
      } catch (ContextClassLoaderException | InterruptedException | IOException
          | NoSuchAlgorithmException | URISyntaxException e) {
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
          contexts.invalidate(contextLocation);
          classloaderFailures.remove(contextLocation);
        } else {
          // failing, but grace period has not elapsed.
          // No need to log anything.
        }
      } finally {
        monitorContext(contextLocation, nextInterval);
      }
    }, interval, TimeUnit.SECONDS);
    LOG.trace("Monitoring context definition file {} for changes at {} second intervals",
        contextLocation, interval);
  }

  // for tests only
  void resetForTests() {
    // Removing the contexts will cause the
    // background monitor task to end
    contexts.invalidateAll();
    contexts.cleanUp();
  }

  @Override
  public void init(ContextClassLoaderEnvironment env) {
    baseCacheDir = requireNonNull(env.getConfiguration().get(Constants.CACHE_DIR_PROPERTY),
        "Property " + Constants.CACHE_DIR_PROPERTY + " not set, cannot create cache directory.");
    String graceProp = env.getConfiguration().get(Constants.UPDATE_FAILURE_GRACE_PERIOD_MINS);
    long graceMins = graceProp == null ? 0 : Long.parseLong(graceProp);
    updateFailureGracePeriodMins = Duration.ofMinutes(graceMins);
    try {
      CacheUtils.createBaseCacheDir(baseCacheDir);
    } catch (IOException | ContextClassLoaderException e) {
      throw new IllegalStateException("Error creating base cache directory at " + baseCacheDir, e);
    }
  }

  @Override
  public ClassLoader getClassLoader(final String contextLocation)
      throws ContextClassLoaderException {
    Preconditions.checkState(baseCacheDir != null, "init not called before calling getClassLoader");
    requireNonNull(contextLocation, "context name must be supplied");
    try {
      final URL contextLocationUrl = new URL(contextLocation);
      final AtomicBoolean newlyCreated = new AtomicBoolean(false);
      final LocalCachingContext ccl = contexts.get(contextLocation, cn -> {
        try {
          ContextDefinition def = parseContextDefinition(contextLocationUrl);
          LocalCachingContext newCcl = new LocalCachingContext(baseCacheDir, def);
          newCcl.initialize();
          newlyCreated.set(true);
          return newCcl;
        } catch (Exception e) {
          throw new RuntimeException("Error creating context classloader", e);
        }
      });
      if (newlyCreated.get()) {
        monitorContext(contextLocation, ccl.getDefinition().getMonitorIntervalSeconds());
      }
      return ccl.getClassloader();
    } catch (MalformedURLException e) {
      throw new ContextClassLoaderException(
          "Expected valid URL to context definition file but received: " + contextLocation, e);
    } catch (RuntimeException re) {
      Throwable t = re.getCause();
      if (t != null && t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (t != null) {
        if (t instanceof ContextClassLoaderException) {
          throw (ContextClassLoaderException) t;
        } else {
          throw new ContextClassLoaderException(t.getMessage(), t);
        }
      } else {
        throw new ContextClassLoaderException(re.getMessage(), re);
      }
    }
  }

}
