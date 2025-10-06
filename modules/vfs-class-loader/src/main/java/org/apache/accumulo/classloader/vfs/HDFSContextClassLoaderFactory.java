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
package org.apache.accumulo.classloader.vfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.spi.common.ContextClassLoaderEnvironment;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.Retry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;

/**
 * A {@link ContextClassLoaderFactory} implementation which uses a {@link URLClassLoader} per
 * defined context. To use this class, need to set the Accumulo configuration property
 * <b>general.context.class.loader.factory</b> to the fully qualified name of this class. <br>
 * <br>
 * Configuration of this class is done by having a base directory in HDFS which stores context
 * directories (e.g., "hdfs://test:1234/contexts" could be the base directory) and where the
 * children are specific contexts (e.g., contextA, contextB). The "contexts" directory should not
 * contain any other files or directories. Each context directory should contain a manifest file
 * <b>manifest.json</b> and JAR files. The manifest file defines the context name and the JAR info
 * for what JARs should be in the {@link URLClassLoader} for that context. For example:
 *
 * <pre>
 * {
 *   "context": "contextA",
 *   "jars": [
 *     {
 *       "name": "Iterators.jar",
 *       "checksum": "f2ca1bb6c7e907d06dafe4687e579fce76b37e4e93b7605022da52e6ccc26fd2"
 *     },
 *     {
 *       "name": "IteratorsV2.jar",
 *       "checksum": "934ee77f70dc82403618c154aa63af6f4ebbe3ac1eaf22df21e7e64f0fb6643d"
 *     }
 *   ]
 * }
 * </pre>
 *
 * Two system properties need to be set to use this class: <br>
 * <b>hdfs.contexts.class.loader.base.dir</b> <br>
 * This defines where the context directories exist within HDFS (e.g., "hdfs://test:1234/contexts"
 * in our example) <br>
 * <b>local.contexts.class.loader.download.dir</b> <br>
 * This defines where the context info will be locally downloaded to. Will only download the JARs.
 * For example, if set to "/path/to/contexts" and
 * {@link HDFSContextClassLoaderFactory#getClassLoader(String)} is called with the argument
 * "contextA", will create the local directory "/path/to/contexts/contextA" (note that all parent
 * directories will be created if needed), will download the JARs Iterators.jar and IteratorsV2.jar
 * from HDFS to the contextA directory. The {@link URLClassLoader} will be constructed from these
 * local JARs, will be associated with "contextA", and will be cached. <br>
 * Another optional system property: <br>
 * <b>manifest.file.check.interval.sec</b> <br>
 * may be set as well. This defines how often (in sec) the cached contexts will be checked and/or
 * updated based on changes to their associated manifest file. If not set, default interval is 10s.
 * Changes to this will take effect on next interval.
 */
public class HDFSContextClassLoaderFactory implements ContextClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSContextClassLoaderFactory.class);
  public static final String MANIFEST_FILE_NAME = "manifest.json";
  private static final String HASH_ALG = "SHA-256";
  private static final long DEFAULT_MANIFEST_CHECK_INTERVAL = TimeUnit.SECONDS.toMillis(10);
  public static final String HDFS_CONTEXTS_BASE_DIR = "hdfs.contexts.class.loader.base.dir";
  public static final String LOCAL_CONTEXTS_DOWNLOAD_DIR =
      "local.contexts.class.loader.download.dir";
  public static final String MANIFEST_FILE_CHECK_INTERVAL = "manifest.file.check.interval.sec";
  // Cache the class loaders for re-use
  // WeakReferences are used so that the class loaders can be cleaned up when no longer needed
  // Classes that are loaded contain a reference to the class loader used to load them
  // so the class loader will be garbage collected when no more classes are loaded that reference it
  private final Cache<Path,Pair<String,URLClassLoader>> classLoaders =
      CacheBuilder.newBuilder().weakValues().build();
  private FileSystem hdfs;
  private Path HDFSContextsDir;
  private java.nio.file.Path localContextsDir;
  private final Configuration hadoopConf = new Configuration();
  private final Thread manifestFileChecker = new Thread(new ManifestFileChecker());
  private final AtomicBoolean shutdownManifestFileChecker = new AtomicBoolean(false);

  @VisibleForTesting
  public class ManifestFileChecker implements Runnable {
    @Override
    public void run() {
      while (!shutdownManifestFileChecker.get()) {
        var manifestFileCheckInterval = System.getProperty(MANIFEST_FILE_CHECK_INTERVAL);
        long sleepTime = manifestFileCheckInterval == null ? DEFAULT_MANIFEST_CHECK_INTERVAL
            : TimeUnit.SECONDS.toMillis(Integer.parseInt(manifestFileCheckInterval));

        classLoaders.asMap().keySet().forEach(HDFSManifestFile -> classLoaders.asMap()
            .computeIfPresent(HDFSManifestFile, (key, existingVal) -> {
              var existingChecksum = existingVal.getFirst();
              try (var HDFSManifestFileIn = hdfs.open(HDFSManifestFile)) {
                var HDFSManifestFileBytes = HDFSManifestFileIn.readAllBytes();
                var computedChecksum = checksum(HDFSManifestFileBytes);
                if (!existingChecksum.equals(computedChecksum)) {
                  // This manifest file has changed since the class loader for it was computed.
                  // Need to update the class loader entry.
                  LOG.debug("HDFS manifest file {} existing checksum {} computed checksum {}",
                      HDFSManifestFile, existingChecksum, computedChecksum);
                  try (var tempState =
                      createTempState(HDFSManifestFile.getParent().getName(), HDFSManifestFile,
                          new ByteArrayInputStream(HDFSManifestFileBytes), computedChecksum)) {
                    return createClassLoader(tempState);
                  }
                }
              } catch (IOException | ContextClassLoaderException | ExecutionException e) {
                LOG.error("Exception occurred in thread {}", Thread.currentThread().getName(), e);
              }
              return existingVal;
            }));

        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
    }
  }

  /**
   * Used to store info needed to create the value in the Cache
   */
  private static class TempState implements AutoCloseable {
    private final String contextName;
    private final java.nio.file.Path tempPath;
    private final java.nio.file.Path finalPath;
    private final String checksum;
    private final URL[] urls;

    private TempState(String contextName, java.nio.file.Path tempPath, java.nio.file.Path finalPath,
        String checksum, URL[] urls) {
      this.contextName = contextName;
      this.tempPath = tempPath;
      this.finalPath = finalPath;
      this.checksum = checksum;
      this.urls = urls;
    }

    @Override
    public void close() throws IOException {
      if (tempPath.toFile().exists()) {
        FileUtils.deleteDirectory(tempPath.toFile());
        LOG.debug("Creating the class loader for context {} did not fully complete. Deleted the "
            + "temp directory {} and all of its contents.", contextName, tempPath);
      }
    }
  }

  @VisibleForTesting
  public void shutdownManifestFileChecker() {
    shutdownManifestFileChecker.set(true);
  }

  @VisibleForTesting
  public static class Context {
    private final String contextName;
    private final List<JarInfo> jars;

    public Context(String contextName, JarInfo... jars) {
      this.contextName = contextName;
      this.jars = List.of(jars);
    }

    public String getContextName() {
      return contextName;
    }

    public List<JarInfo> getJars() {
      return jars;
    }
  }

  @VisibleForTesting
  public static class JarInfo {
    private final String jarName;
    private final String checksum;

    public JarInfo(String jarName, String checksum) {
      this.jarName = jarName;
      this.checksum = checksum;
    }

    public String getJarName() {
      return jarName;
    }

    public String getChecksum() {
      return checksum;
    }
  }

  @Override
  public void init(ContextClassLoaderEnvironment env) {
    HDFSContextsDir = new Path(System.getProperty(HDFS_CONTEXTS_BASE_DIR));
    localContextsDir = java.nio.file.Path.of(System.getProperty(LOCAL_CONTEXTS_DOWNLOAD_DIR));
    // create the directory to store all the context info, if needed
    if (localContextsDir.toFile().mkdirs()) {
      LOG.info("Created dir(s) " + localContextsDir);
    }
    try {
      hdfs = FileSystem.get(HDFSContextsDir.toUri(), hadoopConf);
    } catch (IOException e) {
      throw new IllegalStateException("could not obtain FileSystem for " + HDFSContextsDir.toUri(),
          e);
    }
    manifestFileChecker.start();
  }

  private TempState createTempState(String contextName, Path HDFSManifestFile,
      InputStream HDFSManifestFileIn, String HDFSManifestFileChecksum)
      throws IOException, ContextClassLoaderException, ExecutionException {
    URL[] urls;

    var localFinalContextDir = localContextsDir.resolve(contextName);
    // create the temp directory, will eventually be renamed to the above or deleted
    var localTempContextDir =
        localContextsDir.resolve("tmp-" + contextName + "-" + UUID.randomUUID());

    if (localTempContextDir.toFile().mkdir()) {
      LOG.debug("Created dir " + localTempContextDir);
    } else {
      throw new IOException("Could not create dir " + localTempContextDir);
    }

    // read the context info from the HDFS manifest file and download the jars referenced to the
    // temp directory in the local file system
    try (var reader = new InputStreamReader(HDFSManifestFileIn, StandardCharsets.UTF_8)) {
      Context context = new Gson().fromJson(reader, Context.class);
      urls = new URL[context.getJars().size()];
      int i = 0;
      if (HDFSManifestFile.getParent() != null) {
        for (var jar : context.getJars()) {
          // download to temp dir
          var HDFSJarPath = new Path(HDFSManifestFile.getParent(), jar.getJarName());
          var localTempJarPath = localTempContextDir.resolve(jar.getJarName());
          hdfs.copyToLocalFile(HDFSJarPath, new Path(localTempJarPath.toUri()));
          LOG.info("Copied from {} to {}", HDFSJarPath, localTempJarPath);
          // verify downloaded jar checksum matches what is in the manifest file
          var computedChecksumLocalJar = checksumLocalFile(localTempJarPath);
          if (!computedChecksumLocalJar.equals(jar.getChecksum())) {
            throw new ContextClassLoaderException(contextName,
                new IllegalStateException(String.format(
                    "checksum: %s of downloaded jar: %s (downloaded from %s to %s) did not match "
                        + "checksum present: %s in manifest file: %s. Consider retrying and/or "
                        + "updating the checksum for this jar in the manifest file",
                    computedChecksumLocalJar, jar.getJarName(), HDFSJarPath, localTempJarPath,
                    jar.getChecksum(), HDFSManifestFile)));
          }
          // use final (non-temp) jar path for the URL as that is what will exist if this op
          // completes
          var localFinalJarPath = localFinalContextDir.resolve(jar.getJarName());
          urls[i++] = localFinalJarPath.toUri().toURL();
        }
      } else {
        throw new ContextClassLoaderException(contextName);
      }
    }

    return new TempState(contextName, localTempContextDir, localFinalContextDir,
        HDFSManifestFileChecksum, urls);
  }

  private Pair<String,URLClassLoader> createClassLoader(TempState tempState) throws IOException {
    // if the final path already exists, we are replacing it with temp path, so recursively delete
    // the final path
    if (Files.exists(tempState.finalPath)) {
      FileUtils.deleteDirectory(tempState.finalPath.toFile());
    }
    // rename temp path to final path
    if (tempState.tempPath.toFile().renameTo(tempState.finalPath.toFile())) {
      LOG.info("Renamed {} to {}", tempState.tempPath, tempState.finalPath);
    }
    var classLoader = new URLClassLoader(tempState.urls, ClassLoader.getSystemClassLoader());
    return new Pair<>(tempState.checksum, classLoader);
  }

  public String checksumLocalFile(java.nio.file.Path path) throws IOException {
    try (var in = FileUtils.openInputStream(path.toFile())) {
      return checksum(in.readAllBytes());
    }
  }

  public static String checksum(byte[] fileBytes) {
    try {
      StringBuilder checksum = new StringBuilder();
      for (byte b : MessageDigest.getInstance(HASH_ALG).digest(fileBytes)) {
        checksum.append(String.format("%02x", b));
      }
      return checksum.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public ClassLoader getClassLoader(String contextName) throws ContextClassLoaderException {
    Path HDFSContextPath = new Path(HDFSContextsDir, contextName);
    Path HDFSManifestFile = new Path(HDFSContextPath, MANIFEST_FILE_NAME);

    try (var HDFSManifestFileIn = hdfs.open(HDFSManifestFile)) {
      // if another thread has already started creating the class loader for this context:
      // wait a short time for it to succeed in creating and caching the classloader, otherwise
      // continue and try to create and cache the classloader ourselves (only one result will be
      // cached)
      try (var allContextDirs = Files.list(localContextsDir)) {
        // will match temp directories or final directories for this context name
        if (allContextDirs.map(java.nio.file.Path::getFileName)
            .anyMatch(path -> path.toString().contains(contextName))) {
          var retry = Retry.builder().maxRetries(5).retryAfter(50, TimeUnit.MILLISECONDS)
              .incrementBy(50, TimeUnit.MILLISECONDS).maxWait(1, TimeUnit.SECONDS).backOffFactor(2)
              .logInterval(500, TimeUnit.MILLISECONDS).createRetry();
          final String operationDesc =
              "Waiting for another thread to finish creating/caching the class loader for context "
                  + contextName;
          Pair<String,URLClassLoader> valInCache;
          while ((valInCache = classLoaders.getIfPresent(HDFSManifestFile)) == null
              && retry.canRetry()) {
            retry.useRetry();
            try {
              retry.waitForNextAttempt(LOG, operationDesc);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
          }

          if (valInCache != null) {
            retry.logCompletion(LOG, operationDesc);
            return valInCache.getSecond();
          } else {
            LOG.debug(
                "Operation '" + operationDesc + "' has not yet succeeded. Attempting ourselves.");
          }
        }
      }

      var HDFSManifestFileBytes = HDFSManifestFileIn.readAllBytes();
      try (var tempState = createTempState(contextName, HDFSManifestFile,
          new ByteArrayInputStream(HDFSManifestFileBytes), checksum(HDFSManifestFileBytes))) {
        // atomically return cached value if it exists OR rename the temp dir to the final dir and
        // create and cache the new class loader
        // Guava Cache get() will not work here as that requires the same key to always map to the
        // same value (see documentation). Using the map view as a work-around
        return classLoaders.asMap().computeIfAbsent(HDFSManifestFile, key -> {
          try {
            return createClassLoader(tempState);
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        }).getSecond();
      }
    } catch (IOException | ExecutionException e) {
      throw new ContextClassLoaderException(contextName, e);
    }
  }
}
