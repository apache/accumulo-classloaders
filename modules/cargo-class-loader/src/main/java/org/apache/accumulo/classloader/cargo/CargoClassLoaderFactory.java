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
package org.apache.accumulo.classloader.cargo;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.accumulo.classloader.cargo.LocalStore.HardLinkFailedException;
import org.apache.accumulo.classloader.cargo.manifest.CargoManifest;
import org.apache.accumulo.classloader.cargo.manifest.Resource;
import org.apache.accumulo.core.spi.common.ContextClassLoaderEnvironment;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A ContextClassLoaderFactory implementation. For this implementation, "cargo" is a class loader
 * context based on a JSON-formatted {@link CargoManifest} file at a URL specified in the parameter
 * for the {@link #getClassLoader(String)} method. The manifest contains a monitor interval and a
 * set of {@link Resource} URLs to transport from their remote storage location to the local disk,
 * where it will be used to construct and return a {@link ClassLoader}.
 *
 * <p>
 * The manifest URL will be monitored for updates at the monitor interval specified in the manifest,
 * and the cargo class loader instance will be updated if any changes to the manifest occur. If the
 * monitoring fails for a period configurable with the {@value #PROP_GRACE_PERIOD} property, then
 * monitoring will discontinue until the next time the {@link #getClassLoader(String)} is called for
 * that manifest URL.
 *
 * <p>
 * Each resource in the manifest is defined by a URL to the file, a checksum algorithm, and a
 * checksum.
 *
 * <p>
 * The URLs supplied for the cargo manifest and for the resources may use any URL type with a
 * registered provider in your application, such as: file, http, https, or hdfs.
 *
 * <p>
 * Properties to control this factory's behavior can be passed via
 * {@link #init(ContextClassLoaderEnvironment)} and include the following:
 *
 * <ul>
 * <li>{@value #PROP_CACHE_DIR}: (required) the location of the local storage for holding cargo
 * resources</li>
 * <li>{@value #PROP_ALLOWED_URLS}: (required) a Java regular expression of allowable URLs to access
 * for both manifests and resources</li>
 * <li>{@value #PROP_GRACE_PERIOD}: (optional; default: 0) the number of minutes to tolerate
 * failures while attempting to monitor a manifest URL before causing an error in the application; a
 * value of 0 will tolerate failures indefinitely</li>
 * </ul>
 *
 * <p>
 * The storage directory may be shared by multiple processes using this factory. Temporary files,
 * download-in-progress signal files, hard-links, and atomic file system moves are used to ensure
 * that processes do not conflict with one another, and to ensure network efficiency by avoiding
 * concurrent downloads for the same resource files.
 */
public class CargoClassLoaderFactory implements ContextClassLoaderFactory {

  public static final String PROP_CACHE_DIR = "general.custom.classloader.cargo.cache.dir";

  public static final String PROP_GRACE_PERIOD =
      "general.custom.classloader.cargo.update.grace.minutes";

  public static final String PROP_ALLOWED_URLS =
      "general.custom.classloader.cargo.allowed.urls.pattern";

  private static final Logger LOG = LoggerFactory.getLogger(CargoClassLoaderFactory.class);
  private static final Cleaner CLEANER = Cleaner.create();

  // executor for the monitor tasks
  private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(0);

  // stores the latest seen CargoManifest for a remote URL; String types are used here for the key
  // instead of URL because URL.hashCode could trigger network activity for hostname lookups
  private final ConcurrentHashMap<String,CargoManifest> manifests = new ConcurrentHashMap<>();

  // to keep this coherent with the manifests, updates to this should be done in manifests.compute()
  private final DeduplicationCache<DeduplicationCacheKey,LocalStore,URLClassLoader> classloaders =
      new DeduplicationCache<>(CargoClassLoaderFactory::createClassLoader, Duration.ofHours(24),
          null);

  private final AtomicReference<LocalStore> localStore = new AtomicReference<>();

  private final Map<String,Stopwatch> classloaderFailures = new HashMap<>();
  private volatile Supplier<Duration> updateFailureGracePeriodMins;

  // this is a BiConsumer so we can pass a type in the String
  private volatile BiConsumer<String,URL> allowedUrlChecker;

  /**
   * Schedule a task to execute at {@code interval} seconds to update the cached resources if the
   * CargoManifest has changed. The task schedules a follow-on task at the update interval value (if
   * it changed) and the task is successful or throws a handled exception. When an unhandled
   * exception is thrown, then the corresponding entry in the manifests map is cleared. The next
   * call to {@code #getClassLoader(String)} will recreate the manifests map entry and schedule the
   * monitor task.
   */
  private void monitor(final String url, long interval) {
    LOG.trace("Monitoring cargo manifest {} for changes at {} second intervals", url, interval);
    executor.schedule(() -> {
      try {
        checkMonitoredUrl(url, interval);
      } catch (Throwable t) {
        LOG.error("Unhandled exception occurred in cargo manifest monitor thread. Removing"
            + " cargo manifest {}.", url, t);
        manifests.remove(url);
        throw t;
      }
    }, interval, TimeUnit.SECONDS);
  }

  @Override
  public void init(ContextClassLoaderEnvironment env) {
    String value = requireNonNull(env.getConfiguration().get(PROP_CACHE_DIR),
        "Property " + PROP_CACHE_DIR + " not set, cannot create cache directory.");

    // these suppliers are used so we can update these config properties without restarting,
    // but limit the frequency at which we check the config for the grace period and the url pattern
    var maxFrequency = Duration.ofMinutes(1);
    updateFailureGracePeriodMins = Suppliers.memoizeWithExpiration(() -> {
      String graceProp = env.getConfiguration().get(PROP_GRACE_PERIOD);
      long graceMins = graceProp == null ? 0 : Long.parseLong(graceProp);
      return Duration.ofMinutes(graceMins);
    }, maxFrequency);
    Supplier<
        Pattern> allowedUrlsPattern =
            Suppliers.memoizeWithExpiration(
                () -> Pattern.compile(requireNonNull(env.getConfiguration().get(PROP_ALLOWED_URLS),
                    "Property " + PROP_ALLOWED_URLS + " not set, no URLs are allowed")),
                maxFrequency);

    allowedUrlChecker = (locationType, url) -> {
      var p = allowedUrlsPattern.get();
      Preconditions.checkArgument(p.matcher(url.toExternalForm()).matches(),
          "Cargo %s URL (%s) not allowed by pattern (%s)", locationType, url.toExternalForm(),
          p.pattern());
    };
    try {
      // check the allowed URLs pattern, getting it ready for first use, and warning if it is bad
      allowedUrlsPattern.get();
    } catch (RuntimeException npe) {
      LOG.warn(
          "Property {} is not set or contains an invalid pattern ()."
              + " No ClassLoader instances will be created until it is set.",
          PROP_ALLOWED_URLS, env.getConfiguration().get(PROP_ALLOWED_URLS), npe);
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
  public ClassLoader getClassLoader(final String manifestUrl) throws ContextClassLoaderException {
    Preconditions.checkState(localStore.get() != null,
        "init not called before calling getClassLoader");
    requireNonNull(manifestUrl, "manifest URL must be supplied");
    final var classloader = new AtomicReference<URLClassLoader>();
    try {
      // get the current manifest, or create it from the URL if absent; this has the side effect of
      // creating and caching a class loader instance if it doesn't exist for the computed manifest
      manifests.compute(manifestUrl,
          (key, previous) -> computeManifestAndClassLoader(classloader, key, previous));
    } catch (RuntimeException e) {
      throw new ContextClassLoaderException(e.getMessage(), e);
    }
    return classloader.get();
  }

  private CargoManifest computeManifestAndClassLoader(AtomicReference<URLClassLoader> resultHolder,
      String url, CargoManifest previous) {
    CargoManifest computed;
    if (previous == null) {
      try {
        computed = downloadManifest(url);
        // we can set up monitoring now, but it will be blocked from doing anything yet, until this
        // finishes, since this code and the monitoring code both use manifests.compute(), which
        // is atomic/blocking for the same key
        monitor(url, computed.getMonitorIntervalSeconds());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      computed = previous;
    }
    resultHolder.set(
        classloaders.computeIfAbsent(new DeduplicationCacheKey(url, computed), localStore::get));
    return computed;
  }

  private CargoManifest downloadManifest(String url) throws IOException {
    LOG.trace("Retrieving cargo manifest from {}", url);
    URL urlUrl = new URL(url);
    allowedUrlChecker.accept("manifest", urlUrl);
    return CargoManifest.download(urlUrl);
  }

  private void checkMonitoredUrl(String url, long interval) {
    CargoManifest current = manifests.compute(url, (key, previous) -> {
      if (previous == null) {
        // manifest has been removed from the map, no need to check for update
        LOG.debug("CargoManifest for {} not present, no longer monitoring for changes", url);
        return null;
      }
      // check for any classloaders still in the cache that were created for a cargo manifest
      // found at this URL
      if (!classloaders.anyMatch(cacheKey -> cacheKey.getLocation().equals(url))) {
        LOG.debug("ClassLoader for {} not present, no longer monitoring for changes", url);
        return null;
      }
      return previous;
    });
    if (current == null) {
      return;
    }
    long nextInterval = interval;
    try {
      final CargoManifest update = downloadManifest(url);
      if (!current.getChecksum().equals(update.getChecksum())) {
        LOG.debug("Cargo manifest for {} has changed", url);
        localStore.get().storeCargo(update);
        manifests.put(url, update);
        nextInterval = update.getMonitorIntervalSeconds();
        classloaderFailures.remove(url);
      } else {
        LOG.trace("Cargo manifest for {} has not changed", url);
      }
      // reschedule this task to run if the cargo manifest exists.
      // Atomically lock on the key and only reschedule if the value is present.
      final long finalMonitorInterval = nextInterval;
      manifests.compute(url, (k, v) -> {
        if (v != null) {
          monitor(url, finalMonitorInterval);
        }
        return v;
      });
    } catch (IOException | RuntimeException e) {
      LOG.error("Error parsing updated cargo manifest at {}. Classloader NOT updated!", url, e);
      final Stopwatch failureTimer = classloaderFailures.get(url);
      var gracePeriod = updateFailureGracePeriodMins.get();
      if (gracePeriod.isZero()) {
        // failure monitoring is disabled
        LOG.debug("Property {} not set, not tracking classloader failures for {}",
            PROP_GRACE_PERIOD, url);
      } else if (failureTimer == null) {
        // first failure, start the timer
        classloaderFailures.put(url, Stopwatch.createStarted());
        LOG.debug(
            "Tracking classloader failures for {}, will NOT return working classloader if failures continue for {} minutes",
            url, gracePeriod.toMinutes());
      } else if (failureTimer.elapsed().compareTo(gracePeriod) > 0) {
        // has been failing for the grace period
        // unset the classloader reference so that the failure
        // will return from getClassLoader in the calling thread
        LOG.info("Grace period for failing classloader has elapsed for {}", url);
        manifests.remove(url);
        classloaderFailures.remove(url);
      } else {
        LOG.trace("Failing to update classloader for {} within the grace period", url, e);
      }
      // reschedule this task to run if the cargo manifest exists.
      // Don't put this in finally block as we only want to reschedule
      // on success or handled exception
      // Atomically lock on the key and only reschedule if the value is present.
      final long finalMonitorInterval = nextInterval;
      manifests.compute(url, (k, v) -> {
        if (v != null) {
          monitor(url, finalMonitorInterval);
        }
        return v;
      });
    }
  }

  @SuppressFBWarnings(value = "DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED",
      justification = "doPrivileged is deprecated without replacement and removed in newer Java")
  static URLClassLoader createClassLoader(DeduplicationCacheKey cacheKey, LocalStore localStore) {
    // use a LinkedHashSet to preserve the order of the cargo resources
    final var hardLinks = new LinkedHashSet<Path>();
    Path hardLinksDir = null;

    var manifest = cacheKey.getCargoManifest();

    // stage the downloads before attempting hard link creation
    localStore.storeCargo(manifest);

    // keep trying to hard-link all the resources if the hard-linking fails
    while (hardLinksDir == null) {
      hardLinks.clear();
      try {
        hardLinksDir = localStore.createWorkingHardLinks(manifest, hardLinks::add);
        LOG.trace("Created hard links at {} for {}", hardLinksDir, cacheKey);
      } catch (HardLinkFailedException e) {
        var failedHardLinksDir = e.getDestinationDirectory();
        LOG.warn(
            "Exception creating a hard link in {} due to missing resource {}; attempting re-download of cargo resources",
            failedHardLinksDir, e.getMissingResource(), e);
        try {
          recursiveDelete(failedHardLinksDir);
        } catch (IOException ioe) {
          LOG.warn(
              "Saw exception removing directory {} after hard link creation failure; this should be cleaned up manually",
              failedHardLinksDir, ioe);
        }
        localStore.storeCargo(manifest);
      }
    }

    URL[] urls = hardLinks.stream().map(p -> {
      try {
        LOG.trace("Added resource {} to classpath", p);
        return p.toUri().toURL();
      } catch (MalformedURLException e) {
        // shouldn't be possible, since these are file-based URLs
        throw new UncheckedIOException(e);
      }
    }).toArray(URL[]::new);

    final var cl = new URLClassLoader(cacheKey.toString(), urls,
        CargoClassLoaderFactory.class.getClassLoader());
    LOG.info("New classloader created for {}", cacheKey);

    final var cleanDir = hardLinksDir;
    CLEANER.register(cl, () -> {
      try {
        recursiveDelete(cleanDir);
      } catch (IOException e) {
        LOG.warn("Saw exception when executing cleaner on directory {}", cleanDir, e);
      }
    });
    return cl;
  }

  private static void recursiveDelete(Path directory) throws IOException {
    if (Files.exists(directory)) {
      try (var walker = Files.walk(directory)) {
        walker.map(Path::toFile).sorted(Comparator.reverseOrder()).forEach(File::delete);
      }
    }
  }

}
