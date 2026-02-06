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
package org.apache.accumulo.classloader.ccl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.util.Objects.requireNonNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.accumulo.classloader.ccl.manifest.Manifest;
import org.apache.accumulo.classloader.ccl.manifest.Resource;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A simple storage service backed by a local file system for storing downloaded {@link Manifest}
 * files and the {@link Resource} objects it references.
 *
 * <p>
 * The layout of the storage area consists of three directories. You may create this directory
 * structure in advance, but they must be on the same file system, one which supports atomic moves
 * and hard-links. The directories are:
 * <ul>
 * <li><b>manifests</b>: stores a copy of the {@link Manifest} JSON files for each context, and
 * exist primarily for user convenience (they aren't used again by this factory)
 * <li><b>resources</b>: stores a copy of all the {@link Resource} files for all contexts
 * <li><b>working</b>: files in use, such as temporary files while downloading, and directories of
 * hard-linked resource files for the lifetime of a classloader
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
 * duplicate effort. Concurrent downloads can occur if one download attempt stalls. This is
 * intentional, so that others will not be blocked indefinitely. When multiple attempts to download
 * a file do occur concurrently, they use unique temporary file names and atomic file system moves
 * to ensure correctness, even under these circumstances.
 *
 * <p>
 * Once all files for a context are downloaded, they are hard-linked to a temporary directory where
 * their checksums are verified before use, so that a {@link URLClassLoader} can be constructed
 * using the same resource ordering as in the {@link Manifest} file.
 */
final class LocalStore {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStore.class);
  private static final String PID = Long.toString(ProcessHandle.current().pid());

  private final Path manifestsDir;
  private final Path resourcesDir;
  private final Path workingDir;
  private final BiConsumer<String,URL> allowedUrlChecker;

  public static final String MANIFESTS_DIR = "manifests";
  public static final String RESOURCES_DIR = "resources";
  public static final String WORKING_DIR = "working";

  public LocalStore(final Path baseDir, final BiConsumer<String,URL> allowedUrlChecker)
      throws IOException {
    requireNonNull(baseDir);
    this.allowedUrlChecker = requireNonNull(allowedUrlChecker);
    this.manifestsDir = Files.createDirectories(baseDir.resolve(MANIFESTS_DIR));
    this.resourcesDir = Files.createDirectories(baseDir.resolve(RESOURCES_DIR));
    this.workingDir = Files.createDirectories(baseDir.resolve(WORKING_DIR));
  }

  Path manifestsDir() {
    return manifestsDir;
  }

  Path resourcesDir() {
    return resourcesDir;
  }

  Path workingDir() {
    return workingDir;
  }

  // pattern to match regular files that have at least one non-dot character preceding a dot and a
  // non-zero suffix; these files can be easily converted so the local store retains the original
  // file name extension, while non-matching files will not attempt to retain the original file name
  // extension, and will instead just append the checksum to the original file name
  private static Pattern fileNamesWithExtensionPattern = Pattern.compile("^(.*[^.].*)[.]([^.]+)$");

  public static String localResourceName(Resource r) {
    requireNonNull(r);
    String remoteFileName = r.getFileName();
    String checksum = checksumForFileName(r);
    var matcher = fileNamesWithExtensionPattern.matcher(remoteFileName);
    if (matcher.matches()) {
      return String.format("%s-%s.%s", matcher.group(1), checksum, matcher.group(2));
    }
    return String.format("%s-%s", remoteFileName, checksum);
  }

  private static String checksumForFileName(String algorithm, String checksum) {
    return algorithm.replace('/', '_') + "-" + checksum;
  }

  static String checksumForFileName(Manifest manifest) {
    return checksumForFileName(manifest.getChecksumAlgorithm(), manifest.getChecksum());
  }

  static String checksumForFileName(Resource resource) {
    return checksumForFileName(resource.getAlgorithm(), resource.getChecksum());
  }

  // creates a new empty file with a unique name, for use as a temporary file
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "the working directory is intentionally controlled by the user config")
  private Path createTempFile(String baseName) {
    try {
      return Files.createTempFile(workingDir, "PID_" + PID + "_" + baseName + "_", ".tmp");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // creates a new empty directory with a unique name, for use as a temporary directory
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "the working directory is intentionally controlled by the user config")
  private Path createTempDirectory(String baseName) {
    try {
      return Files.createTempDirectory(workingDir, "PID_" + PID + "_" + baseName + "_");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Save the {@link Manifest} to the manifests directory, and all of its resources to the resources
   * directory.
   */
  void storeContext(final Manifest manifest) {
    requireNonNull(manifest, "manifest must be supplied");
    final String destinationName = checksumForFileName(manifest) + ".json";
    try {
      storeManifest(manifest, destinationName);
      int waitingOnOtherDownloadsCount;
      do {
        waitingOnOtherDownloadsCount = 0;
        for (Resource resource : manifest.getResources()) {
          Path path = storeResource(resource);
          if (path == null) {
            LOG.trace("Skipped resource {} while another process or thread is downloading it",
                resource.getLocation());
            waitingOnOtherDownloadsCount++;
            continue;
          }
        }
        // avoid rapid cycling checking for other downloads to finish; wait longer if more downloads
        // are being waited on, but no more than 1 second total
        Thread.sleep(Math.min(waitingOnOtherDownloadsCount * 100, 1_000));
      } while (waitingOnOtherDownloadsCount > 0);
    } catch (IOException e) {
      LOG.error("Error storing resources for {}", destinationName, e);
      throw new UncheckedIOException(e);
    } catch (RuntimeException e) {
      LOG.error("Error storing resources for {}", destinationName, e);
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private void storeManifest(final Manifest manifest, final String destinationName)
      throws IOException {
    Path destinationPath = manifestsDir.resolve(destinationName);
    if (Files.exists(destinationPath)) {
      return;
    }
    // Avoid colliding with other processes by saving to a unique temp name first
    Path tempPath = createTempFile(destinationName);
    Files.write(tempPath, manifest.toJson().getBytes(UTF_8));
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
    final String baseName = localResourceName(resource);
    final Path destinationPath = resourcesDir.resolve(baseName);
    final Path downloadingProgressPath = workingDir.resolve(baseName + ".downloading");

    if (Files.exists(destinationPath)) {
      LOG.trace("Resource {} is already cached at {}", url, destinationPath);
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

    final Path tempPath = createTempFile(baseName);
    var task = new FutureTask<Void>(() -> downloadFile(tempPath, resource), null);
    var t = new Thread(task);
    t.setDaemon(true);
    t.setName("downloading " + url + " to " + tempPath);

    LOG.trace("Storing remote resource {} locally at {} via temp file {}", url, destinationPath,
        tempPath);
    t.start();
    try {
      while (!task.isDone()) {
        try {
          Files.write(downloadingProgressPath, PID.getBytes(UTF_8));
        } catch (IOException e) {
          LOG.warn(
              "Error writing progress file {}. Other processes may attempt to download the same file concurrently.",
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

  private static final int DL_BUFF_SIZE = 1024 * 1024;

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "user-supplied URL is the intended functionality")
  private void downloadFile(Path tempPath, Resource resource) {
    URL url = resource.getLocation();
    allowedUrlChecker.accept("resource", url);

    // SYNC ensures file integrity on each write, in case of system failure. Buffering minimizes
    // system calls te read/write data which minimizes the number of syncs.
    try (var in = new BufferedInputStream(url.openStream(), DL_BUFF_SIZE);
        var out = new BufferedOutputStream(Files.newOutputStream(tempPath, SYNC), DL_BUFF_SIZE)) {
      in.transferTo(out);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    verifyDownload(resource, tempPath, () -> Files.delete(tempPath));
  }

  private void verifyDownload(Resource resource, Path downloadPath, Closeable cleanUpAction) {
    final String algorithm = resource.getAlgorithm();
    final String checksum;
    try {
      checksum = new DigestUtils(algorithm).digestAsHex(downloadPath);
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to perform checksum verification on " + downloadPath
          + " for resource " + resource.getLocation(), e);
    }
    if (!resource.getChecksum().equals(checksum)) {
      var ise = new IllegalStateException(
          "Checksum " + checksum + " for resource " + resource.getLocation()
              + " does not match checksum in the manifest " + resource.getChecksum());
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

  Path createWorkingHardLinks(final Manifest manifest, Consumer<Path> forEachLink)
      throws HardLinkFailedException {
    Path hardLinkDir = createTempDirectory("context-" + checksumForFileName(manifest));
    // create all hard links first
    for (Resource r : manifest.getResources()) {
      String fileName = localResourceName(r);
      Path p = resourcesDir.resolve(fileName);
      try {
        Path hardLink = hardLinkDir.resolve(fileName);
        LOG.trace("Creating hard link {} for resource {}", hardLink, r.getLocation());
        Files.createLink(hardLink, p);
      } catch (NoSuchFileException e) {
        throw new HardLinkFailedException(hardLinkDir, p, e);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    // verify checksums
    for (Resource r : manifest.getResources()) {
      String fileName = localResourceName(r);
      Path hardLink = hardLinkDir.resolve(fileName);
      LOG.trace("Verifying checksum of hard link {} for resource {}", hardLink, r.getLocation());
      verifyDownload(r, hardLink, null);
      forEachLink.accept(hardLink);
    }
    return hardLinkDir;
  }

  static class HardLinkFailedException extends Exception {

    private static final long serialVersionUID = 1L;
    private final Path destDir;
    private Path missingResource;

    HardLinkFailedException(Path destDir, Path missingResource, NoSuchFileException cause) {
      super("Creating hard link in directory " + destDir + " failed", cause);
      this.destDir = destDir;
      this.missingResource = missingResource;
    }

    Path getDestinationDirectory() {
      return destDir;
    }

    Path getMissingResource() {
      return missingResource;
    }

  }

}
