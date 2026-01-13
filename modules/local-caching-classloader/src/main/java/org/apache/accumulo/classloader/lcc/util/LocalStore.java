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
package org.apache.accumulo.classloader.lcc.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.classloader.lcc.util.LccUtils.DIGESTER;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple storage service backed by a local file system for storing downloaded
 * {@link ContextDefinition} files and the {@link Resource} objects it references.
 * <p>
 * The layout of the storage area consists of two directories:
 * <ul>
 * <li><b>contexts</b> stores a copy of the {@link ContextDefinition} JSON files for each context,
 * and exist primarily for user convenience (they aren't used again by this factory)
 * <li><b>resources</b> stores a copy of all the {@link Resource} files for all contexts
 * </ul>
 *
 * <p>
 * Files downloaded to these directories use a naming convention that includes their checksum, so
 * that each unique file will be stored exactly once, regardless of how many threads, processes, or
 * contexts reference the file.
 *
 * <p>
 * Downloads make a best effort attempt to avoid multiple processes or threads from downloading the
 * same file at the same time, using a temporary signal file with the suffix ".downloading" to avoid
 * duplicate effort. Duplicate effort and race conditions can occur if one download attempt stalls.
 * This is intentional, so that others will not be blocked indefinitely. When multiple attempts to
 * download a file do occur concurrently, they use unique temporary file names and atomic file
 * system moves to ensure correctness, even under these circumstances.
 *
 * <p>
 * Once all files for a context are downloaded, an array of {@link URL}s will be returned, which
 * point to the local files that have been downloaded for the context, in the same order as
 * specified in the {@link ContextDefinition} file, to be used for constructing a
 * {@link URLClassLoader}.
 */
public final class LocalStore {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStore.class);
  private static final String PID = Long.toString(ProcessHandle.current().pid());

  private final Path contextsDir;
  private final Path resourcesDir;

  public LocalStore(final Path baseDir) throws IOException {
    this.contextsDir = requireNonNull(baseDir).toAbsolutePath().resolve("contexts");
    this.resourcesDir = baseDir.resolve("resources");
    Files.createDirectories(contextsDir);
    Files.createDirectories(resourcesDir);
  }

  Path contextsDir() {
    return contextsDir;
  }

  Path resourcesDir() {
    return resourcesDir;
  }

  // pattern to match regular files that have at least one non-dot character preceding a dot and a
  // non-zero suffix; these files can be easily converted so the local store retains the original
  // file name extension, while non-matching files will not attempt to retain the original file name
  // extension, and will instead just append the checksum to the original file name
  private static Pattern fileNamesWithExtensionPattern = Pattern.compile("^(.*[^.].*)[.]([^.]+)$");

  static String localName(String remoteFileName, String checksum) {
    requireNonNull(remoteFileName);
    requireNonNull(checksum);
    var matcher = fileNamesWithExtensionPattern.matcher(remoteFileName);
    if (matcher.matches()) {
      return String.format("%s-%s.%s", matcher.group(1), checksum, matcher.group(2));
    }
    return String.format("%s-%s", remoteFileName, checksum);
  }

  private static String tempName(String baseName) {
    return "." + requireNonNull(baseName) + "_PID" + PID + "_" + UUID.randomUUID() + ".tmp";
  }

  /**
   * Save the {@link ContextDefinition} to the contexts directory, and all of its resources to the
   * resources directory.
   */
  public URL[] storeContextResources(final ContextDefinition contextDefinition) throws IOException {
    requireNonNull(contextDefinition, "definition must be supplied");
    // use a LinkedHashSet to preserve the order of the context resources
    final Set<Path> localFiles = new LinkedHashSet<>();
    // store it with a .json suffix, if the original file didn't have one
    final String origSourceName = contextDefinition.getSourceFileName();
    final String sourceNameWithSuffix =
        origSourceName.toLowerCase().endsWith(".json") ? origSourceName : origSourceName + ".json";
    final String destinationName = localName(sourceNameWithSuffix, contextDefinition.getChecksum());
    try {
      storeContextDefinition(contextDefinition, destinationName);
      boolean successful = false;
      while (!successful) {
        localFiles.clear();
        for (Resource resource : contextDefinition.getResources()) {
          Path path = storeResource(resource);
          if (path == null) {
            LOG.debug("Skipped resource {} while another process or thread is downloading it",
                resource.getLocation());
            continue;
          }
          localFiles.add(path);
          LOG.trace("Added resource {} to classpath", path);
        }
        successful = localFiles.size() == contextDefinition.getResources().size();
      }

    } catch (IOException | RuntimeException e) {
      LOG.error("Error storing resources for context {}", destinationName, e);
      throw e;
    }
    return localFiles.stream().map(p -> {
      try {
        return p.toUri().toURL();
      } catch (MalformedURLException e) {
        // this shouldn't happen since these are local file paths
        throw new UncheckedIOException(e);
      }
    }).toArray(URL[]::new);
  }

  private void storeContextDefinition(final ContextDefinition contextDefinition,
      final String destinationName) throws IOException {
    Path destinationPath = contextsDir.resolve(destinationName);
    if (Files.exists(destinationPath)) {
      return;
    }
    Path tempPath = contextsDir.resolve(tempName(destinationName));
    // the temporary file name should be unique for this attempt, but CREATE_NEW is used here, along
    // with the subsequent ATOMIC_MOVE, to guarantee we don't collide with any other task saving the
    // same file
    Files.write(tempPath, contextDefinition.toJson().getBytes(UTF_8), CREATE_NEW);
    Files.move(tempPath, destinationPath, ATOMIC_MOVE);
  }

  /*
   * If the resource is already downloaded, attempt cleanup of old ".downloading" files and return.
   * If it needs to be downloaded, attempt to create a ".downloading" file to signal progress. If
   * that file already exists and has been updated within the last 30 seconds, return null to signal
   * to the calling code to wait and retry later.
   *
   * Once downloading begins, update the ".downloading" file with the current PID every 5 seconds so
   * long as the file is still being downloaded, so that others will wait on this to finish instead
   * of starting a duplicate attempt.
   *
   * Failures to download are not re-attempted, but will propagate up to the caller.
   */
  private Path storeResource(final Resource resource) throws IOException {
    final URL url = resource.getLocation();
    final FileResolver source = FileResolver.resolve(url);
    final String baseName = localName(source.getFileName(), resource.getChecksum());
    final Path destinationPath = resourcesDir.resolve(baseName);
    final Path tempPath = resourcesDir.resolve(tempName(baseName));
    final Path downloadingProgressPath = resourcesDir.resolve("." + baseName + ".downloading");

    if (Files.exists(destinationPath)) {
      LOG.trace("Resource {} is already cached at {}", url, destinationPath);
      verifyDownload(resource, destinationPath, null);
      try {
        // clean up any in progress files that may have been left behind by previous failed attempts
        Files.deleteIfExists(downloadingProgressPath);
      } catch (IOException e) {
        // this is a best effort, and it doesn't matter if we fail
        LOG.trace("Unable to clean up an old progress file {}", downloadingProgressPath, e);
      }
      return destinationPath;
    }

    try {
      if (System.currentTimeMillis() - Files.getLastModifiedTime(downloadingProgressPath).toMillis()
          < 30_000 || Files.deleteIfExists(downloadingProgressPath)) {
        return null;
      }
    } catch (NoSuchFileException e) {
      // this is okay, nobody else is downloading the file, so we can try
    }

    try {
      // CREATE_NEW forces an exception if the file already exists, so we can avoid colliding with
      // others attempts to start progress on the same resource file
      Files.write(downloadingProgressPath, PID.getBytes(UTF_8), CREATE_NEW);
    } catch (FileAlreadyExistsException e) {
      // somebody else beat us to it, let them try to download it; we'll check back later
      return null;
    }

    var task = new FutureTask<Void>(() -> downloadFile(source, tempPath, resource), null);
    var t = new Thread(task);
    t.setDaemon(true);
    t.setName("downloading " + url + " to " + tempPath);

    LOG.trace("Storing remote resource {} locally at {} via temp file {}", url, destinationPath,
        tempPath);
    t.start();
    try {
      while (!task.isDone()) {
        try {
          Files.write(downloadingProgressPath, PID.getBytes(UTF_8), TRUNCATE_EXISTING);
        } catch (IOException e) {
          LOG.warn(
              "Error writing progress file {}. Other processes may attempt downloading the same file.",
              downloadingProgressPath, e);
        }
        try {
          task.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          // timeout while waiting for task to complete; do nothing; keep waiting
          LOG.trace("Still making progress downloading {}", tempPath);
        } catch (InterruptedException e) {
          task.cancel(true);
          Thread.currentThread().interrupt();
          throw new IllegalStateException(
              "Thread was interrupted while waiting on resource to copy from " + url + " to "
                  + tempPath,
              e);
        } catch (ExecutionException e) {
          throw new IllegalStateException("Error copying resource from " + url + " to " + tempPath,
              e);
        }
      }

      // ATOMIC_MOVE is used to guarantee we don't collide with any other task saving the same file
      Files.move(tempPath, destinationPath, ATOMIC_MOVE);
      LOG.debug("Successfully downloaded {}", destinationPath);

      return destinationPath;
    } finally {
      task.cancel(true);

      try {
        Files.deleteIfExists(downloadingProgressPath);
      } catch (IOException e) {
        // if we can't clean up the downloading progress file, it doesn't matter, because the
        // destination file has already been created; retries from other processes check that first
        LOG.debug("Error deleting the downloading progress file (probably doesn't matter)", e);
      }

      if (t.isAlive()) {
        LOG.debug("Unexpectedly found download thread " + t.getId()
            + " still alive (thread was likely interrupted): " + t.getName());
      }
    }
  }

  private void downloadFile(FileResolver source, Path tempPath, Resource resource) {
    // CREATE_NEW ensures the temporary file name is unique for this attempt
    // SYNC ensures file integrity on each write, in case of system failure
    try (var in = source.getInputStream();
        var out = Files.newOutputStream(tempPath, CREATE_NEW, WRITE, SYNC)) {
      in.transferTo(out);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    verifyDownload(resource, tempPath, () -> Files.delete(tempPath));
  }

  private void verifyDownload(Resource resource, Path downloadPath, Closeable cleanUpAction) {
    final String checksum;
    try {
      checksum = DIGESTER.digestAsHex(downloadPath);
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to perform checksum verification on " + downloadPath
          + " for resource " + resource.getLocation(), e);
    }
    if (!resource.getChecksum().equals(checksum)) {
      var ise = new IllegalStateException(
          "Checksum " + checksum + " for resource " + resource.getLocation()
              + " does not match checksum in context definition " + resource.getChecksum());
      if (cleanUpAction != null) {
        try {
          cleanUpAction.close();
        } catch (IOException e) {
          ise.addSuppressed(e);
        }
      }
      throw ise;
    }
  }
}
