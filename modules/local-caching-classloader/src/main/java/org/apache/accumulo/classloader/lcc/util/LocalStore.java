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
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.accumulo.classloader.lcc.Constants;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  static String tempName(String baseName) {
    return "." + requireNonNull(baseName) + ".tmp_" + PID;
  }

  public URLClassLoaderParams storeContextResources(final ContextDefinition contextDefinition)
      throws IOException {
    requireNonNull(contextDefinition, "definition must be supplied");
    final String contextName = contextDefinition.getLocalFileName();
    // use a LinkedHashSet to preserve the order of the context resources
    final Set<Path> localFiles = new LinkedHashSet<>();
    try {
      storeContextDefinition(contextDefinition);
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
      LOG.error("Error initializing context: " + contextName, e);
      throw e;
    }
    return new URLClassLoaderParams(contextName + "_" + contextDefinition.getChecksum(),
        localFiles.stream().map(p -> {
          try {
            return p.toUri().toURL();
          } catch (MalformedURLException e) {
            // this shouldn't happen since these are local file paths
            throw new UncheckedIOException(e);
          }
        }).toArray(URL[]::new));
  }

  private void storeContextDefinition(final ContextDefinition contextDefinition)
      throws IOException {
    // context names could contain anything, so let's remove any path separators that would mess
    // with the file names
    String destinationName =
        localName(contextDefinition.getLocalFileName(), contextDefinition.getChecksum());
    Path destinationPath = contextsDir.resolve(destinationName);
    if (Files.exists(destinationPath)) {
      return;
    }
    Path tempPath = contextsDir.resolve(tempName(destinationName));
    Files.write(tempPath, contextDefinition.toJson().getBytes(UTF_8));
    Files.move(tempPath, destinationPath, ATOMIC_MOVE);
  }

  private Path storeResource(final Resource resource) {
    final URL url = resource.getLocation();
    final FileResolver source;
    try {
      source = FileResolver.resolve(url);
    } catch (IOException e) {
      // there was an error getting the resolver for the resource location url
      return null;
    }
    final String baseName = localName(source.getFileName(), resource.getChecksum());
    final Path destinationPath = resourcesDir.resolve(baseName);
    final Path tempPath = resourcesDir.resolve(tempName(baseName));
    final Path inProgressPath = resourcesDir.resolve("." + baseName + ".downloading");

    if (Files.exists(destinationPath)) {
      LOG.trace("Resource {} is already cached at {}", url, destinationPath);
      return destinationPath;
    }

    try {
      Files.write(inProgressPath, PID.getBytes(UTF_8), StandardOpenOption.CREATE_NEW);
    } catch (FileAlreadyExistsException e) {
      // TODO try this, and check the timestamp to see if it has made recent progress
      // if no recent progress, delete the file and return null
      // for now, return null and assume it will be finished by the other process later
      return null;
    } catch (IOException e) {
      // some other exception occurred that we don't know how to handle; will attempt retry
      return null;
    }

    LOG.trace("Storing remote resource {} locally at {}", url, destinationPath);
    try (InputStream is = source.getInputStream()) {
      // TODO update the in progress file as we make progress during the copy; maybe a background
      // thread, or maybe X number of bytes written
      Files.copy(is, tempPath, REPLACE_EXISTING);
      final String checksum = Constants.getChecksummer().digestAsHex(tempPath);
      if (!resource.getChecksum().equals(checksum)) {
        LOG.error(
            "Checksum {} for resource {} does not match checksum in context definition {}, removing cached copy.",
            checksum, url, resource.getChecksum());
        Files.delete(tempPath);
        throw new IllegalStateException("Checksum " + checksum + " for resource " + url
            + " does not match checksum in context definition " + resource.getChecksum());
      }
      Files.move(tempPath, destinationPath, ATOMIC_MOVE);
    } catch (IOException e) {
      LOG.error("Error copying resource from {} to {}. Retrying...", url, destinationPath, e);
    } finally {
      try {
        Files.deleteIfExists(inProgressPath);
      } catch (IOException e) {
        // if we can't clean up this file and the file is already downloaded, then this doesn't
        // matter; however, if the file isn't already downloaded, then this file's existence will
        // only temporarily block others from attempting to download; so, this is fine to ignore
        // TODO maybe log the failure to clean up
      }
    }
    return destinationPath;
  }

}
