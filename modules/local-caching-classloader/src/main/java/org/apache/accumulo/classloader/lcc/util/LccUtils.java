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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.util.LocalStore.HardLinkFailedException;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class LccUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LccUtils.class);

  private static final ConcurrentHashMap<String,DigestUtils> DIGESTERS = new ConcurrentHashMap<>();
  private static final Cleaner CLEANER = Cleaner.create();

  // keep at most one DigestUtils instance for each algorithm
  public static DigestUtils getDigester(String algorithm) {
    return DIGESTERS.computeIfAbsent(algorithm, DigestUtils::new);
  }

  private static String checksumForFileName(String algorithm, String checksum) {
    return algorithm.replace('/', '_') + "-" + checksum;
  }

  public static String checksumForFileName(ContextDefinition definition) {
    return checksumForFileName(definition.getChecksumAlgorithm(), definition.getChecksum());
  }

  public static String checksumForFileName(Resource definition) {
    return checksumForFileName(definition.getAlgorithm(), definition.getChecksum());
  }

  @SuppressFBWarnings(value = "DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED",
      justification = "doPrivileged is deprecated without replacement and removed in newer Java")
  public static URLClassLoader createClassLoader(ContextCacheKey cacheKey, LocalStore localStore) {
    // use a LinkedHashSet to preserve the order of the context resources
    final var hardLinks = new LinkedHashSet<Path>();
    Path hardLinksDir = null;

    var def = cacheKey.getContextDefinition();

    // stage the downloads before attempting hard link creation
    localStore.storeContextResources(def);

    // keep trying to hard-link all the resources if the hard-linking fails
    while (hardLinksDir == null) {
      hardLinks.clear();
      try {
        hardLinksDir = localStore.createWorkingHardLinks(def, hardLinks::add);
        LOG.trace("Created hard links at {} for context {}", hardLinksDir, cacheKey);
      } catch (HardLinkFailedException e) {
        var failedHardLinksDir = e.getDestinationDirectory();
        LOG.warn(
            "Exception creating a hard link in {} due to missing resource {}; attempting re-download of context resources",
            failedHardLinksDir, e.getMissingResource(), e);
        try {
          LccUtils.recursiveDelete(failedHardLinksDir);
        } catch (IOException ioe) {
          LOG.warn(
              "Saw exception removing directory {} after hard link creation failure; this should be cleaned up manually",
              failedHardLinksDir, ioe);
        }
        localStore.storeContextResources(def);
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
        LocalCachingContextClassLoaderFactory.class.getClassLoader());
    LOG.info("New classloader created for {}", cacheKey);

    final var cleanDir = hardLinksDir;
    CLEANER.register(cl, () -> {
      try {
        LccUtils.recursiveDelete(cleanDir);
      } catch (IOException e) {
        LOG.warn("Saw exception when executing cleaner on directory {}", cleanDir, e);
      }
    });
    return cl;
  }

  public static void recursiveDelete(Path directory) throws IOException {
    if (Files.exists(directory)) {
      try (var walker = Files.walk(directory)) {
        walker.map(Path::toFile).sorted(Comparator.reverseOrder()).forEach(File::delete);
      }
    }
  }

}
