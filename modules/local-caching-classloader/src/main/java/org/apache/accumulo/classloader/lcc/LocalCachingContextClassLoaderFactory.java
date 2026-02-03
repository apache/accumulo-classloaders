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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.util.ContextCacheKey;
import org.apache.accumulo.classloader.lcc.util.DeduplicationCache;
import org.apache.accumulo.classloader.lcc.util.LccUtils;
import org.apache.accumulo.classloader.lcc.util.LccUtils.URLClassLoaderParams;
import org.apache.accumulo.classloader.lcc.util.LocalStore;
import org.apache.accumulo.core.spi.common.ContextClassLoaderEnvironment;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;

/**
 * A ContextClassLoaderFactory implementation that creates and maintains a ClassLoader for a context
 * definition at a remote URL, and referencing remotely stored resources. This factory expects the
 * parameter passed to {@link #getClassLoader(String)} to be the URL of a JSON-formatted
 * {@link ContextDefinition} file. The file contains an interval at which this class should monitor
 * the file for changes and a list of {@link Resource} objects. If the monitoring fails for a period
 * configurable with the {@link #UPDATE_FAILURE_GRACE_PERIOD_MINS} property, then monitoring will
 * discontinue until the next use of that context. Each resource is defined by a URL to the file, a
 * checksum algorithm, and a checksum.
 * <p>
 * The URLs supplied for the context definition file and for the resources may use any URL type with
 * a registered provider in your application, such as: file, http, https, or hdfs.
 * <p>
 * As this class processes the ContextDefinition, it fetches the contents of the resource from the
 * resource URL and caches it in a directory on the local filesystem. This class uses the value of
 * the property {@link #CACHE_DIR_PROPERTY} passed via {@link #init(ContextClassLoaderEnvironment)}
 * as the root directory and creates a sub-directory for context definition files, and another for
 * resource files. All cached files have a name that includes their checksum. The required property,
 * {@link #ALLOWED_URLS_PATTERN}, is used to specify a pattern for allowed URLs to be fetched.
 * <p>
 * An in-progress signal file is used for each resource file while it is being downloaded, to allow
 * multiple processes or threads to try to avoid redundant downloads. Atomic filesystem moves are
 * used to guarantee correctness if multiple downloads do occur.
 * <p>
 * Note that because the cache directory is shared among multiple processes, and one process can't
 * know what the other processes are doing, this class cannot clean up the shared cache directory.
 * It is left to the user to remove unused old files.
 */
public class LocalCachingContextClassLoaderFactory implements ContextClassLoaderFactory {

  public static final String CACHE_DIR_PROPERTY = "general.custom.classloader.lcc.cache.dir";

  public static final String UPDATE_FAILURE_GRACE_PERIOD_MINS =
      "general.custom.classloader.lcc.update.grace.minutes";

  public static final String ALLOWED_URLS_PATTERN =
      "general.custom.classloader.lcc.allowed.urls.pattern";

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalCachingContextClassLoaderFactory.class);

  private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(0);

  // stores the latest seen ContextDefinition for a remote URL location; String types are used here
  // for the key instead of URL because URL.hashCode could trigger network activity for hostname
  // lookups
  private final ConcurrentHashMap<String,ContextDefinition> contextDefs = new ConcurrentHashMap<>();

  // to keep this coherent with the contextDefs, updates to this should be done in the compute
  // method of contextDefs
  private final DeduplicationCache<ContextCacheKey,URLClassLoaderParams,
      URLClassLoader> classloaders =
          new DeduplicationCache<>(LccUtils::createClassLoader, Duration.ofHours(24), null);

  private final AtomicReference<LocalStore> localStore = new AtomicReference<>();

  private final Map<String,Stopwatch> classloaderFailures = new HashMap<>();
  private volatile Duration updateFailureGracePeriodMins;

  // this is a BiConsumer so we can pass a type in the String
  private volatile BiConsumer<String,URL> allowedUrlChecker;

  /**
   * Schedule a task to execute at {@code interval} seconds to update the LocalCachingContext if the
   * ContextDefinition has changed. The task schedules a follow-on task at the update interval value
   * (if it changed) and the task is successful or throws a handled exception. When an unhandled
   * exception is thrown, then the corresponding entry in the contextDefs map is cleared. The next
   * call to {@code #getClassLoader(String)} will recreate the contextDefs map entry and schedule
   * the monitor task.
   */
  private void monitorContext(final String contextLocation, long interval) {
    LOG.trace("Monitoring context definition file {} for changes at {} second intervals",
        contextLocation, interval);
    executor.schedule(() -> {
      try {
        checkMonitoredLocation(contextLocation, interval);
      } catch (Throwable t) {
        LOG.error("Unhandled exception occurred in context definition monitor thread. Removing"
            + " context definition {}.", contextLocation, t);
        contextDefs.remove(contextLocation);
        throw t;
      }
    }, interval, TimeUnit.SECONDS);
  }

  @Override
  public void init(ContextClassLoaderEnvironment env) {
    String value = requireNonNull(env.getConfiguration().get(CACHE_DIR_PROPERTY),
        "Property " + CACHE_DIR_PROPERTY + " not set, cannot create cache directory.");
    String graceProp = env.getConfiguration().get(UPDATE_FAILURE_GRACE_PERIOD_MINS);
    long graceMins = graceProp == null ? 0 : Long.parseLong(graceProp);
    updateFailureGracePeriodMins = Duration.ofMinutes(graceMins);
    // limit the frequency at which we check the config and re-compile the pattern
    Supplier<Pattern> allowedUrlsPattern = Suppliers.memoizeWithExpiration(
        () -> Pattern.compile(requireNonNull(env.getConfiguration().get(ALLOWED_URLS_PATTERN),
            "Property " + ALLOWED_URLS_PATTERN + " not set, no URLs are allowed")),
        Duration.ofMinutes(1));
    allowedUrlChecker = (locationType, url) -> {
      var p = allowedUrlsPattern.get();
      Preconditions.checkArgument(p.matcher(url.toExternalForm()).matches(),
          "%s location (%s) not allowed by pattern (%s)", locationType, url.toExternalForm(),
          p.pattern());
    };
    try {
      // check the allowed URLs pattern, getting it ready for first use, and warning if it is bad
      allowedUrlsPattern.get();
    } catch (RuntimeException npe) {
      LOG.warn(
          "Property {} is not set or contains an invalid pattern ()."
              + " No ClassLoader instances will be created until it is set.",
          ALLOWED_URLS_PATTERN, env.getConfiguration().get(ALLOWED_URLS_PATTERN), npe);
    }
    final Path baseCacheDir;
    if (value.startsWith("file:")) {
      try {
        baseCacheDir = Path.of(new URL(value).toURI());
      } catch (IOException | URISyntaxException e) {
        throw new IllegalArgumentException(
            "Malformed file: URL specified for base directory: " + value, e);
      }
    } else if (value.startsWith("/")) {
      baseCacheDir = Path.of(value);
    } else {
      throw new IllegalArgumentException(
          "Base directory is neither a file URL nor an absolute file path: " + value);
    }
    try {
      localStore.set(new LocalStore(baseCacheDir, allowedUrlChecker));
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to create the local storage area at " + baseCacheDir,
          e);
    }
  }

  @Override
  public ClassLoader getClassLoader(final String contextLocation)
      throws ContextClassLoaderException {
    Preconditions.checkState(localStore.get() != null,
        "init not called before calling getClassLoader");
    requireNonNull(contextLocation, "context location must be supplied");
    final var classloader = new AtomicReference<URLClassLoader>();
    try {
      // get the current definition, or create it from the location if it doesn't exist; this has
      // the side effect of creating and caching a URLClassLoader instance if it doesn't exist for
      // the computed definition
      contextDefs.compute(contextLocation,
          (contextLocationKey, previousDefinition) -> computeDefinitionAndClassLoader(classloader,
              contextLocationKey, previousDefinition));
    } catch (RuntimeException e) {
      throw new ContextClassLoaderException(e.getMessage(), e);
    }
    return classloader.get();
  }

  private ContextDefinition computeDefinitionAndClassLoader(
      AtomicReference<URLClassLoader> resultHolder, String contextLocation,
      ContextDefinition previousDefinition) {
    ContextDefinition computedDefinition;
    if (previousDefinition == null) {
      try {
        computedDefinition = getDefinition(contextLocation);
        // we can set up monitoring now, but it will be blocked from doing anything yet, until this
        // finishes, since this code and the monitoring code both use contextDefs.compute(), which
        // is atomic/blocking for the same key
        monitorContext(contextLocation, computedDefinition.getMonitorIntervalSeconds());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      computedDefinition = previousDefinition;
    }
    final URLClassLoader classloader =
        classloaders.computeIfAbsent(new ContextCacheKey(contextLocation, computedDefinition),
            (Supplier<URLClassLoaderParams>) () -> localStore.get()
                .storeContextResources(computedDefinition));
    resultHolder.set(classloader);
    return computedDefinition;
  }

  private ContextDefinition getDefinition(String contextLocation) throws IOException {
    LOG.trace("Retrieving context definition file from {}", contextLocation);
    URL url = new URL(contextLocation);
    allowedUrlChecker.accept("Context definition", url);
    return ContextDefinition.fromRemoteURL(url);
  }

  private void checkMonitoredLocation(String contextLocation, long interval) {
    ContextDefinition currentDef =
        contextDefs.compute(contextLocation, (contextLocationKey, previousDefinition) -> {
          if (previousDefinition == null) {
            // context has been removed from the map, no need to check for update
            LOG.debug(
                "ContextDefinition for context {} not present, no longer monitoring for changes",
                contextLocation);
            return null;
          }
          // check for any classloaders still in the cache that were created for a context
          // definition found at this URL
          if (!classloaders.anyMatch(cacheKey -> cacheKey.getLocation().equals(contextLocation))) {
            LOG.debug("ClassLoader for context {} not present, no longer monitoring for changes",
                contextLocation);
            return null;
          }
          return previousDefinition;
        });
    if (currentDef == null) {
      return;
    }
    long nextInterval = interval;
    try {
      final ContextDefinition update = getDefinition(contextLocation);
      if (!currentDef.getChecksum().equals(update.getChecksum())) {
        LOG.debug("Context definition for {} has changed", contextLocation);
        localStore.get().storeContextResources(update);
        contextDefs.put(contextLocation, update);
        nextInterval = update.getMonitorIntervalSeconds();
        classloaderFailures.remove(contextLocation);
      } else {
        LOG.trace("Context definition for {} has not changed", contextLocation);
      }
      // reschedule this task to run if the context definition exists.
      // Atomically lock on the context key and only reschedule if the context is present.
      final long finalMonitorInterval = nextInterval;
      contextDefs.compute(contextLocation, (k, v) -> {
        if (v != null) {
          monitorContext(contextLocation, finalMonitorInterval);
        }
        return v;
      });
    } catch (IOException | RuntimeException e) {
      LOG.error("Error parsing updated context definition at {}. Classloader NOT updated!",
          contextLocation, e);
      final Stopwatch failureTimer = classloaderFailures.get(contextLocation);
      if (updateFailureGracePeriodMins.isZero()) {
        // failure monitoring is disabled
        LOG.debug("Property {} not set, not tracking classloader failures for context {}",
            UPDATE_FAILURE_GRACE_PERIOD_MINS, contextLocation);
      } else if (failureTimer == null) {
        // first failure, start the timer
        classloaderFailures.put(contextLocation, Stopwatch.createStarted());
        LOG.debug(
            "Tracking classloader failures for context {}, will NOT return working classloader if failures continue for {} minutes",
            contextLocation, updateFailureGracePeriodMins.toMinutes());
      } else if (failureTimer.elapsed().compareTo(updateFailureGracePeriodMins) > 0) {
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
      // reschedule this task to run if the context definition exists.
      // Don't put this in finally block as we only want to reschedule
      // on success or handled exception
      // Atomically lock on the context key and only reschedule if the context is present.
      final long finalMonitorInterval = nextInterval;
      contextDefs.compute(contextLocation, (k, v) -> {
        if (v != null) {
          monitorContext(contextLocation, finalMonitorInterval);
        }
        return v;
      });
    }
  }

}
