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
package org.apache.accumulo.classloader.hdfs;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
import com.google.gson.Gson;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link ContextClassLoaderFactory} implementation which uses a {@link URLClassLoader} per
 * defined context. To use this class, need to set the Accumulo configuration property
 * <b>general.context.class.loader.factory</b> to the fully qualified name of this class. <br>
 * <br>
 * Configuration of this class is done by having a base directory in HDFS which stores context
 * directories. It must be named "contexts" (e.g., "hdfs://test:1234/contexts" could be the base
 * directory). The children are specific contexts (names can be anything e.g., "contextA"). The
 * "contexts" directory should not contain any other files or directories. Each context directory
 * should contain a manifest file <b>manifest.json</b> and JAR files. The context directory defines
 * the context name (e.g., "contextA") and the manifest file defines the JAR info for what JARs
 * should be in the {@link URLClassLoader} for that context. For example:
 *
 * <pre>
 * {
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
 * directories will be created if needed), and will download the JARs Iterators.jar and
 * IteratorsV2.jar from HDFS to the contextA directory. The {@link URLClassLoader} will be
 * constructed from these local JARs, will be associated with "contextA", and will be cached. When
 * the class loader is no longer in use, it may be garbage collected at any point, at which point it
 * will be removed from the cache and the local context directory (including JARs) will be deleted.
 */
public class HDFSContextClassLoaderFactory implements ContextClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSContextClassLoaderFactory.class);
  public static final String MANIFEST_FILE_NAME = "manifest.json";
  private static final String HASH_ALG = "SHA-256";
  public static final String HDFS_CONTEXTS_BASE_DIR = "hdfs.contexts.class.loader.base.dir";
  public static final String LOCAL_CONTEXTS_DOWNLOAD_DIR =
      "local.contexts.class.loader.download.dir";
  // Save the class loaders for re-use
  @VisibleForTesting
  ClassLoaderStore<Path,FinalState> classLoaders;
  private FileSystem hdfs;
  private Path hdfsContextsDir;
  private java.nio.file.Path localContextsDir;
  private MessageDigest messageDigest;
  private final Configuration hadoopConf = new Configuration();
  private final AtomicBoolean shutdownStoreCleaner = new AtomicBoolean(false);

  @VisibleForTesting
  class StoreCleaner implements Runnable {
    private final long intervalMs;

    StoreCleaner(long intervalMs) {
      this.intervalMs = intervalMs;
    }

    @Override
    public void run() {
      while (!shutdownStoreCleaner.get()) {
        classLoaders.removeIf(entry -> entry.getValue().shouldClear());
        try {
          Thread.sleep(intervalMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
    }
  }

  @VisibleForTesting
  void shutdownStoreCleaner() {
    shutdownStoreCleaner.set(true);
  }

  /**
   * Used to store info needed to create the value in the map
   */
  private static final class TempState implements AutoCloseable {
    private final String contextName;
    private final java.nio.file.Path localTempContextDir;
    private final java.nio.file.Path localFinalContextDir;
    private final long hdfsManifestLastMod;
    private final String hdfsManifestChecksum;
    private final URL[] urls;

    private TempState(String contextName, java.nio.file.Path localTempContextDir,
        java.nio.file.Path localFinalContextDir, long hdfsManifestLastMod,
        String hdfsManifestChecksum, URL[] urls) {
      this.contextName = contextName;
      this.localTempContextDir = localTempContextDir;
      this.localFinalContextDir = localFinalContextDir;
      this.hdfsManifestLastMod = hdfsManifestLastMod;
      this.hdfsManifestChecksum = hdfsManifestChecksum;
      this.urls = urls;
    }

    @Override
    public void close() throws IOException {
      if (localTempContextDir.toFile().exists()) {
        FileUtils.deleteDirectory(localTempContextDir.toFile());
        LOG.debug("Creating the class loader for context {} did not fully complete. Deleted the "
            + "temp directory {} and all of its contents.", contextName, localTempContextDir);
      }
    }
  }

  private static final class FinalState {
    // WeakReference in order to allow for GC when no longer referenced/used elsewhere
    private final WeakReference<URLClassLoader> classLoaderWeakRef;
    private final long hdfsManifestLastMod;
    private final String hdfsManifestChecksum;

    private FinalState(URLClassLoader classLoader, long hdfsManifestLastMod,
        String hdfsManifestChecksum) {
      this.classLoaderWeakRef = new WeakReference<>(classLoader);
      this.hdfsManifestLastMod = hdfsManifestLastMod;
      this.hdfsManifestChecksum = hdfsManifestChecksum;
    }

    private URLClassLoader getClassLoader() {
      return classLoaderWeakRef.get();
    }

    private boolean shouldClear() {
      return classLoaderWeakRef.get() == null;
    }

    @Override
    public String toString() {
      var classLoader = getClassLoader();
      if (classLoader == null) {
        return getClass().getSimpleName() + "[marked for removal]";
      }
      return getClass().getSimpleName() + "[loader object: " + classLoader + ", loader URLs: "
          + Arrays.toString(classLoader.getURLs()) + ", manifest file checksum: "
          + hdfsManifestChecksum + ", last modification: " + new Date(hdfsManifestLastMod) + "]";
    }
  }

  @VisibleForTesting
  static class Context {
    private final List<JarInfo> jars;

    Context(JarInfo... jars) {
      this.jars = List.of(jars);
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "returned list is not modifiable")
    List<JarInfo> getJars() {
      return jars;
    }
  }

  @VisibleForTesting
  static class JarInfo {
    private final String jarName;
    private final String checksum;

    JarInfo(String jarName, String checksum) {
      this.jarName = jarName;
      this.checksum = checksum;
    }

    String getJarName() {
      return jarName;
    }

    String getChecksum() {
      return checksum;
    }
  }

  @Override
  public void init(ContextClassLoaderEnvironment env) {
    hdfsContextsDir = new Path(System.getProperty(HDFS_CONTEXTS_BASE_DIR));
    localContextsDir = java.nio.file.Path.of(System.getProperty(LOCAL_CONTEXTS_DOWNLOAD_DIR));
    // create the directory to store all the context info, if needed
    if (localContextsDir.toFile().mkdirs()) {
      LOG.info("Created dir(s) " + localContextsDir);
    }
    classLoaders = new ClassLoaderStore<>(entry -> {
      var context = entry.getKey().getParent().getName();
      var localContextDir = localContextsDir.resolve(context);
      try {
        FileUtils.deleteDirectory(localContextDir.toFile());
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    });
    try {
      hdfs = FileSystem.get(hdfsContextsDir.toUri(), hadoopConf);
      messageDigest = MessageDigest.getInstance(HASH_ALG);
    } catch (IOException e) {
      throw new IllegalStateException("could not obtain FileSystem for " + hdfsContextsDir.toUri(),
          e);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("no such algorithm: " + HASH_ALG, e);
    }
    var storeCleaner = new Thread(new StoreCleaner(TimeUnit.SECONDS.toMillis(30)));
    storeCleaner.start();
  }

  private TempState createTempState(String contextName, Path hdfsManifestFile,
      InputStream hdfsManifestFileIn, long hdfsManifestFileModTime, String hdfsManifestFileChecksum)
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
    try (var reader = new InputStreamReader(hdfsManifestFileIn, StandardCharsets.UTF_8)) {
      Context context = new Gson().fromJson(reader, Context.class);
      urls = new URL[context.getJars().size()];
      int i = 0;
      if (hdfsManifestFile.getParent() != null) {
        for (var jar : context.getJars()) {
          // download to temp dir
          var hdfsJarPath = new Path(hdfsManifestFile.getParent(), jar.getJarName());
          var localTempJarPath = localTempContextDir.resolve(jar.getJarName());
          hdfs.copyToLocalFile(hdfsJarPath, new Path(localTempJarPath.toUri()));
          LOG.info("Copied from {} to {}", hdfsJarPath, localTempJarPath);
          // verify downloaded jar checksum matches what is in the manifest file
          var computedChecksumLocalJar = checksumLocalFile(localTempJarPath);
          if (!computedChecksumLocalJar.equals(jar.getChecksum())) {
            throw new ContextClassLoaderException(contextName,
                new IllegalStateException(String.format(
                    "checksum: %s of downloaded jar: %s (downloaded from %s to %s) did not match "
                        + "checksum present: %s in manifest file: %s. Consider retrying and/or "
                        + "updating the checksum for this jar in the manifest file",
                    computedChecksumLocalJar, jar.getJarName(), hdfsJarPath, localTempJarPath,
                    jar.getChecksum(), hdfsManifestFile)));
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
        hdfsManifestFileModTime, hdfsManifestFileChecksum, urls);
  }

  /**
   * Finalizes the creation of the class loader. This must be done atomically per context. Returns
   * both the {@link URLClassLoader} and the {@link FinalState} to maintain a strong reference to
   * the loader ({@link FinalState} stores the loader only in a {@link WeakReference}). Need to
   * maintain a strong reference until the loader is returned to the user (at which point, they will
   * have a strong reference). This prevents the GC from cleaning up the loader before it is
   * returned to the user.
   */
  private Pair<URLClassLoader,FinalState> createClassLoader(TempState tempState)
      throws IOException {
    // if the final path already exists, we are replacing it with temp path, so recursively delete
    // the final path
    if (Files.exists(tempState.localFinalContextDir)) {
      FileUtils.deleteDirectory(tempState.localFinalContextDir.toFile());
    }
    if (tempState.localTempContextDir.toFile().renameTo(tempState.localFinalContextDir.toFile())) {
      LOG.info("Renamed {} to {}", tempState.localTempContextDir, tempState.localFinalContextDir);
    }
    var classLoader = new URLClassLoader(tempState.urls, ClassLoader.getSystemClassLoader());
    return new Pair<>(classLoader,
        new FinalState(classLoader, tempState.hdfsManifestLastMod, tempState.hdfsManifestChecksum));
  }

  private String checksumLocalFile(java.nio.file.Path path) throws IOException {
    try (var in = FileUtils.openInputStream(path.toFile())) {
      return checksum(in);
    }
  }

  @VisibleForTesting
  String checksum(InputStream fileStream) throws IOException {
    StringBuilder checksum = new StringBuilder();
    try {
      // optimization: clone if we can (MessageDigest is not thread safe: can't use directly)
      computeChecksum((MessageDigest) messageDigest.clone(), fileStream, checksum);
    } catch (CloneNotSupportedException cnse) {
      // otherwise fall back to creating a new MessageDigest
      try {
        computeChecksum(MessageDigest.getInstance(HASH_ALG), fileStream, checksum);
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException(e);
      }
    }
    return checksum.toString();
  }

  private void computeChecksum(MessageDigest messageDigest, InputStream fileStream,
      StringBuilder checksum) throws IOException {
    try (BufferedInputStream bis = new BufferedInputStream(fileStream)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = bis.read(buffer)) != -1) {
        messageDigest.update(buffer, 0, bytesRead);
      }
      for (byte b : messageDigest.digest()) {
        checksum.append(String.format("%02x", b));
      }
    }
  }

  @Override
  public ClassLoader getClassLoader(String contextName) throws ContextClassLoaderException {
    Path hdfsContextPath = new Path(hdfsContextsDir, contextName);
    Path hdfsManifestFile = new Path(hdfsContextPath, MANIFEST_FILE_NAME);
    // exists to keep the class loader strongly referenced in method scope
    @SuppressWarnings("WriteOnlyObject")
    AtomicReference<URLClassLoader> classLoaderAtomicRef = new AtomicReference<>();

    // check if value is already present in the store for given context and potentially update it
    // (if the manifest file for the context has changed since it was originally stored)
    var computeIfPresentRes =
        classLoaders.computeIfPresent(hdfsManifestFile, (key, existingVal) -> {
          var existingModTime = existingVal.hdfsManifestLastMod;
          long computedModTime = 0L;
          // check file metadata first to avoid reading the whole file unless necessary
          try {
            computedModTime = hdfs.getFileStatus(hdfsManifestFile).getModificationTime();
          } catch (IOException e) {
            LOG.error("Exception occurred in thread {}", Thread.currentThread().getName(), e);
          }
          if (computedModTime > existingModTime) {
            var existingChecksum = existingVal.hdfsManifestChecksum;
            try (var hdfsManifestFileIn = hdfs.open(hdfsManifestFile)) {
              // readAllBytes is okay - manifest files should be small
              var hdfsManifestFileBytes = hdfsManifestFileIn.readAllBytes();
              var computedChecksum = checksum(new ByteArrayInputStream(hdfsManifestFileBytes));
              if (!existingChecksum.equals(computedChecksum)) {
                // This manifest file has changed since the class loader for it was computed.
                // Need to update the class loader entry.
                LOG.debug("HDFS manifest file {} existing checksum {} computed checksum {}",
                    hdfsManifestFile, existingChecksum, computedChecksum);
                try (var tempState = createTempState(hdfsManifestFile.getParent().getName(),
                    hdfsManifestFile, new ByteArrayInputStream(hdfsManifestFileBytes),
                    computedModTime, computedChecksum)) {
                  var classLoaderAndFinalState = createClassLoader(tempState);
                  classLoaderAtomicRef.set(classLoaderAndFinalState.getFirst());
                  return classLoaderAndFinalState.getSecond();
                }
              }
            } catch (IOException | ContextClassLoaderException | ExecutionException e) {
              LOG.error("Exception occurred in thread {}", Thread.currentThread().getName(), e);
            }
          }
          return existingVal;
        });
    if (computeIfPresentRes != null) {
      return computeIfPresentRes.getClassLoader();
    }

    // if another thread has already started creating the class loader for this context:
    // wait a short time for it to succeed in creating and caching the classloader, otherwise
    // continue and try to create and store the classloader ourselves (only one result will be
    // stored)
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
        FinalState valInStore;
        while ((valInStore = classLoaders.get(hdfsManifestFile)) == null && retry.canRetry()) {
          retry.useRetry();
          try {
            retry.waitForNextAttempt(LOG, operationDesc);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        }

        if (valInStore != null) {
          if (retry.retriesCompleted() > 0) {
            retry.logCompletion(LOG, operationDesc);
          }
          return valInStore.getClassLoader();
        } else {
          LOG.debug(
              "Operation '" + operationDesc + "' has not yet succeeded. Attempting ourselves.");
        }
      }
    } catch (IOException e) {
      throw new ContextClassLoaderException(contextName, e);
    }

    try (var hdfsManifestFileIn = hdfs.open(hdfsManifestFile)) {
      // readAllBytes is okay - manifest files should be small
      var hdfsManifestFileBytes = hdfsManifestFileIn.readAllBytes();
      try (var tempState = createTempState(contextName, hdfsManifestFile,
          new ByteArrayInputStream(hdfsManifestFileBytes),
          hdfs.getFileStatus(hdfsManifestFile).getModificationTime(),
          checksum(new ByteArrayInputStream(hdfsManifestFileBytes)))) {
        // atomically return stored value if it exists OR rename the temp dir to the final dir and
        // create and store the new class loader
        return classLoaders.computeIfAbsent(hdfsManifestFile, key -> {
          try {
            var classLoaderAndFinalState = createClassLoader(tempState);
            classLoaderAtomicRef.set(classLoaderAndFinalState.getFirst());
            return classLoaderAndFinalState.getSecond();
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        }).getClassLoader();
      }
    } catch (IOException | ExecutionException e) {
      throw new ContextClassLoaderException(contextName, e);
    }
  }
}
