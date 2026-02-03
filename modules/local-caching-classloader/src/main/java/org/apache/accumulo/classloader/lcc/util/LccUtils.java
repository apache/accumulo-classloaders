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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
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
  public static URLClassLoader createClassLoader(ContextCacheKey cacheKey,
      URLClassLoaderParams params) {
    Path hardLinkDir = params.tempDirCreator
        .apply("context-" + checksumForFileName(cacheKey.getContextDefinition()) + "_");
    URL[] hardLinksAsURLs = new URL[params.paths.size()];
    int i = 0;
    for (Path p : params.paths) {
      try {
        var hardLink = Files.createLink(hardLinkDir.resolve(p.getFileName()), p);
        hardLinksAsURLs[i++] = hardLink.toUri().toURL();
      } catch (NoSuchFileException e) {
        throw new UncheckedIOException("File was found deleted from " + p
            + " while attempting to create a hard link in " + hardLinkDir, e);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    final var cl = new URLClassLoader(cacheKey.toString(), hardLinksAsURLs,
        LocalCachingContextClassLoaderFactory.class.getClassLoader());
    LOG.info("New classloader created for {}", cacheKey);
    CLEANER.register(cl, () -> {
      try {
        LccUtils.recursiveDelete(hardLinkDir);
      } catch (IOException e) {
        LOG.warn("Saw exception when executing cleaner", e);
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

  public static class URLClassLoaderParams {
    private final Set<Path> paths;
    private final Function<String,Path> tempDirCreator;

    public URLClassLoaderParams(LinkedHashSet<Path> paths, Function<String,Path> tempDirCreator) {
      this.paths = Collections.unmodifiableSet(paths);
      this.tempDirCreator = tempDirCreator;
    }
  }

}
