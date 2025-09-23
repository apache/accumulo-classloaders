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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;

public class SimpleHDFSClassLoaderFactory implements ContextClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleHDFSClassLoaderFactory.class);
  // TODO KEVIN RATHBUN see URLContextClassLoaderFactory from main
  // Cache the class loaders for re-use
  // WeakReferences are used so that the class loaders can be cleaned up when no longer needed
  // Classes that are loaded contain a reference to the class loader used to load them
  // so the class loader will be garbage collected when no more classes are loaded that reference it
  private final Cache<Path,URLClassLoader> classLoaders =
      CacheBuilder.newBuilder().weakValues().build();
  // TODO KEVIN RATHBUN need to get from a new configurable prop
  private final Path userDefDir = Path.of(System.getProperty("user.dir"), "target", "contexts");
  private static final String MANIFEST_FILE_NAME = "manifest.json";

  @VisibleForTesting
  public static class Context {
    private final String contextName;
    private final List<JarInfo> jars;

    public Context(String contextName, List<JarInfo> jars) {
      this.contextName = contextName;
      this.jars = List.copyOf(jars);
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

  public SimpleHDFSClassLoaderFactory(AccumuloConfiguration conf) {
    try {
      initializeContexts(conf);
    } catch (IOException e) {
      throw new IllegalStateException(this.getClass().getSimpleName() + " could not be created.",
          e);
    }
  }

  private void initializeContexts(AccumuloConfiguration conf) throws IOException {
    if (classLoaders.size() != 0) {
      LOG.debug("Contexts already initialized, skipping...");
      return;
    }
    final Set<Path> allContextPaths;
    try (var childPathsStream = Files.list(userDefDir)) {
      allContextPaths = childPathsStream.filter(Files::isDirectory).collect(Collectors.toSet());
    }
    for (var contextPath : allContextPaths) {
      Path manifestFile = contextPath.resolve(MANIFEST_FILE_NAME);
      createClassLoader(manifestFile);
    }
  }

  private void createClassLoader(Path manifestFile) throws IOException {
    Context jsonData = new Gson().fromJson(Files.newBufferedReader(manifestFile), Context.class);
    URL[] urls = new URL[jsonData.getJars().size()];
    int i = 0;
    for (var jar : jsonData.getJars()) {
      var manifestFileParent = manifestFile.getParent();
      if (manifestFileParent != null) {
        urls[i++] = manifestFileParent.resolve(jar.getJarName()).toUri().toURL();
      } else {
        throw new IllegalStateException("Parent path of " + manifestFile + " does not exist.");
      }
    }
    classLoaders.put(manifestFile, new URLClassLoader(urls, ClassLoader.getSystemClassLoader()));
  }

  @Override
  public ClassLoader getClassLoader(String contextName) throws ContextClassLoaderException {
    var expectedPath = userDefDir.resolve(contextName).resolve(MANIFEST_FILE_NAME);
    var classLoader = classLoaders.getIfPresent(expectedPath);
    if (classLoader == null) {
      throw new ContextClassLoaderException(contextName);
    }
    return classLoader;
  }
}
