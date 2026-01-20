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
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.jmx.ContextClassLoaders;
import org.apache.accumulo.classloader.lcc.jmx.ContextClassLoadersMXBean;
import org.apache.accumulo.classloader.lcc.util.DeduplicationCache;
import org.apache.accumulo.classloader.lcc.util.LccUtils;
import org.apache.accumulo.classloader.lcc.util.LocalStore;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ContextClassLoaderEnvironment;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * A ContextClassLoaderFactory implementation that creates and maintains a ClassLoader for a context
 * definition at a remote URL, and referencing remotely stored resources. This factory expects the
 * parameter passed to {@link #getClassLoader(String)} to be the URL of a JSON-formatted
 * {@link ContextDefinition} file. The file contains an interval at which this class should monitor
 * the file for changes and a list of {@link Resource} objects. If the monitoring fails for a period
 * configurable with the {@link #UPDATE_FAILURE_GRACE_PERIOD_MINS} property, then monitoring will
 * discontinue until the next use of that context. Each resource is defined by a URL to the file and
 * an expected SHA-256 checksum.
 * <p>
 * The URLs supplied for the context definition file and for the resources can use one of the
 * following URL schemes: file, http, https, or hdfs.
 * <p>
 * As this class processes the ContextDefinition, it fetches the contents of the resource from the
 * resource URL and caches it in a directory on the local filesystem. This class uses the value of
 * the property {@link #CACHE_DIR_PROPERTY} passed via {@link #init(ContextClassLoaderEnvironment)}
 * as the root directory and creates a sub-directory for context definition files, and another for
 * resource files. All cached files have a name that includes their checksum.
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

  private class ContextClassLoaderThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setName("LocalCachingContextClassLoaderMonitor");
      t.setDaemon(true);
      t.setUncaughtExceptionHandler((thread, exception) -> {
        LOG.error("Uncaught exception occurred in monitor thread. Clearing waiting monitoring tasks"
            + " and clearing current context definitions.", exception);
        handleUncaughtExceptionInMonitor();
      });
      return t;
    }
  }

  public static final String CACHE_DIR_PROPERTY =
      Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + "classloader.lcc.cache.dir";

  public static final String UPDATE_FAILURE_GRACE_PERIOD_MINS =
      Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + "classloader.lcc.update.grace.minutes";

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalCachingContextClassLoaderFactory.class);

  private final ScheduledThreadPoolExecutor executor =
      new ScheduledThreadPoolExecutor(1, new ContextClassLoaderThreadFactory());

  // Lock used to block access to the contextDefs object when an uncaught exception
  // occurs in a monitor thread.
  private final ReentrantReadWriteLock cleanupLock = new ReentrantReadWriteLock(true);

  // stores the latest seen ContextDefinition for a remote URL location; String types are used here
  // for the key instead of URL because URL.hashCode could trigger network activity for hostname
  // lookups
  private final ConcurrentHashMap<String,ContextDefinition> contextDefs = new ConcurrentHashMap<>();

  // to keep this coherent with the contextDefs, updates to this should be done in the compute
  // method of contextDefs
  private static final DeduplicationCache<String,URL[],URLClassLoader> classloaders =
      new DeduplicationCache<>(LccUtils::createClassLoader, Duration.ofHours(24), null);

  private final AtomicReference<LocalStore> localStore = new AtomicReference<>();

  private final Map<String,Stopwatch> classloaderFailures = new HashMap<>();
  private volatile Duration updateFailureGracePeriodMins;

  public LocalCachingContextClassLoaderFactory() {
    executor.allowCoreThreadTimeOut(false);
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  /**
   * Schedule a task to execute at {@code interval} seconds to update the LocalCachingContext if the
   * ContextDefinition has changed. The task schedules a follow-on task at the update interval value
   * (if it changed).
   */
  private void monitorContext(final String contextLocation, long interval) {
    LOG.trace("Monitoring context definition file {} for changes at {} second intervals",
        contextLocation, interval);
    executor.schedule(() -> checkMonitoredLocation(contextLocation, interval), interval,
        TimeUnit.SECONDS);
  }

  @Override
  public void init(ContextClassLoaderEnvironment env) {
    String value = requireNonNull(env.getConfiguration().get(CACHE_DIR_PROPERTY),
        "Property " + CACHE_DIR_PROPERTY + " not set, cannot create cache directory.");
    String graceProp = env.getConfiguration().get(UPDATE_FAILURE_GRACE_PERIOD_MINS);
    long graceMins = graceProp == null ? 0 : Long.parseLong(graceProp);
    updateFailureGracePeriodMins = Duration.ofMinutes(graceMins);
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
      localStore.set(new LocalStore(baseCacheDir));
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to create the local storage area at " + baseCacheDir,
          e);
    }
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      mbs.registerMBean(new ContextClassLoaders(), ContextClassLoadersMXBean.getObjectName());
    } catch (MalformedObjectNameException | MBeanRegistrationException
        | NotCompliantMBeanException e) {
      throw new IllegalStateException("Error registering MBean", e);
    } catch (InstanceAlreadyExistsException e) {
      // instance was re-init'd. This is likely to happen during tests
      // can ignore as no issue here
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
      cleanupLock.readLock().lock();
      try {
        contextDefs.compute(contextLocation,
            (contextLocationKey, previousDefinition) -> computeDefinitionAndClassLoader(classloader,
                contextLocationKey, previousDefinition));
      } finally {
        cleanupLock.readLock().unlock();
      }
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
    final URLClassLoader classloader = classloaders.computeIfAbsent(
        newCacheKey(contextLocation, computedDefinition.getChecksum()), (Supplier<URL[]>) () -> {
          try {
            return localStore.get().storeContextResources(computedDefinition);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
    resultHolder.set(classloader);
    return computedDefinition;
  }

  private static ContextDefinition getDefinition(String contextLocation) throws IOException {
    LOG.trace("Retrieving context definition file from {}", contextLocation);
    URL url = new URL(contextLocation);
    return ContextDefinition.fromRemoteURL(url);
  }

  private static String newCacheKey(String contextLocation, String contextChecksum) {
    // the checksum can't contain '-', so everything before the last one is the location
    return contextLocation + "-" + contextChecksum;
  }

  private static boolean cacheKeyMatchesContextLocation(String cacheKey, String contextLocation) {
    // the checksum can't contain '-', so everything before the last one is the location
    // we can't just use startsWith(contextLocation) because there may be other contextLocations
    // that contain this contextLocation as a prefix, so we do an exact match on the location part
    return cacheKey.substring(0, cacheKey.lastIndexOf('-')).equals(contextLocation);
  }

  private void checkMonitoredLocation(String contextLocation, long interval) {
    cleanupLock.readLock().lock();
    try {
      ContextDefinition currentDef =
          contextDefs.compute(contextLocation, (contextLocationKey, previousDefinition) -> {
            if (previousDefinition == null) {
              return null;
            }
            // check for any classloaders still in the cache that were created for a context
            // definition found at this URL
            if (!classloaders
                .anyMatch(cacheKey -> cacheKeyMatchesContextLocation(cacheKey, contextLocation))) {
              LOG.debug("ClassLoader for context {} not present, no longer monitoring for changes",
                  contextLocation);
              return null;
            }
            return previousDefinition;
          });
      if (currentDef == null) {
        // context has been removed from the map, no need to check for update
        LOG.debug("ContextDefinition for context {} not present, no longer monitoring for changes",
            contextLocation);
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
      } finally {
        monitorContext(contextLocation, nextInterval);
      }
    } finally {
      cleanupLock.readLock().unlock();
    }
  }

  public static Map<String,List<String>> getReferencedFiles() {
    final Map<String,List<String>> referencedContexts = new HashMap<>();
    classloaders.forEach((cacheKey, cl) -> {
      List<String> files = new ArrayList<>();
      for (URL u : cl.getURLs()) {
        files.add(u.toString());
      }
      referencedContexts.put(cacheKey, files);
    });
    return referencedContexts;
  }

  private void handleUncaughtExceptionInMonitor() {
    cleanupLock.writeLock().lock();
    try {
      BlockingQueue<Runnable> q = executor.getQueue();
      List<Runnable> tasks = new ArrayList<>(q.size());
      q.drainTo(tasks);
      tasks.forEach(q::remove);
      tasks.clear();
      contextDefs.clear();
    } finally {
      cleanupLock.writeLock().unlock();
    }
  }
}
