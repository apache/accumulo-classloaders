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
import java.util.List;

import org.apache.accumulo.core.spi.common.ContextClassLoaderEnvironment;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
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
 * This defines where the context info will be locally downloaded to. This includes the manifest
 * file(s) and JAR(s). For example, if set to "/path/to/contexts" and
 * {@link HDFSContextClassLoaderFactory#getClassLoader(String)} is called with the argument
 * "contextA", will create the local directory "/path/to/contexts/contextA" (note that all parent
 * directories will be created if needed), will download the manifest file from HDFS to the contextA
 * directory, and will download the JARs Iterators.jar and IteratorsV2.jar from HDFS to the contextA
 * directory. The {@link URLClassLoader} will be constructed from these local JARs, will be
 * associated with "contextA", and will be cached.
 */
public class HDFSContextClassLoaderFactory implements ContextClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSContextClassLoaderFactory.class);
  public static final String MANIFEST_FILE_NAME = "manifest.json";
  public static final String HDFS_CONTEXTS_BASE_DIR = "hdfs.contexts.class.loader.base.dir";
  public static final String LOCAL_CONTEXTS_DOWNLOAD_DIR =
      "local.contexts.class.loader.download.dir";
  // Cache the class loaders for re-use
  // WeakReferences are used so that the class loaders can be cleaned up when no longer needed
  // Classes that are loaded contain a reference to the class loader used to load them
  // so the class loader will be garbage collected when no more classes are loaded that reference it
  private final Cache<Path,URLClassLoader> classLoaders =
      CacheBuilder.newBuilder().weakValues().build();
  private FileSystem fs;
  private final Configuration hadoopConf = new Configuration();

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
    var hdfsContextsBaseDir = new Path(System.getProperty(HDFS_CONTEXTS_BASE_DIR)).toUri();
    try {
      fs = FileSystem.get(hdfsContextsBaseDir, hadoopConf);
    } catch (IOException e) {
      throw new IllegalStateException("could not obtain FileSystem for path " + hdfsContextsBaseDir,
          e);
    }
  }

  private URLClassLoader createClassLoader(String contextName, Path HDFSManifestFile)
      throws IOException, ContextClassLoaderException {
    var localContextsDownloadDirStr = System.getProperty(LOCAL_CONTEXTS_DOWNLOAD_DIR);
    var localContextDownloadDir = java.nio.file.Path.of(localContextsDownloadDirStr, contextName);
    if (localContextDownloadDir.toFile().mkdirs()) {
      LOG.info("Created dir(s) " + localContextDownloadDir);
    }

    var localManifestFile = localContextDownloadDir.resolve(MANIFEST_FILE_NAME);
    fs.copyToLocalFile(HDFSManifestFile, new Path(localManifestFile.toUri()));
    LOG.info("Copied manifest file from HDFS {} to local {}", HDFSManifestFile, localManifestFile);

    // read the context info from the local manifest file, download the jars referenced from HDFS
    // to local file system, and create the URLClassLoader containing these local jars
    Context context =
        new Gson().fromJson(Files.newBufferedReader(localManifestFile), Context.class);
    var HDFSManifestFileParent = HDFSManifestFile.getParent();
    var localManifestFileParent = localManifestFile.getParent();
    URL[] urls = new URL[context.getJars().size()];
    int i = 0;
    for (var jar : context.getJars()) {
      if (HDFSManifestFileParent != null && localManifestFileParent != null) {
        var HDFSJarPath = new Path(HDFSManifestFileParent, jar.getJarName());
        var localJarPath = localManifestFileParent.resolve(jar.getJarName());
        var localJarPathURI = localJarPath.toUri();
        fs.copyToLocalFile(HDFSJarPath, new Path(localJarPathURI));
        urls[i++] = localJarPathURI.toURL();
      } else {
        throw new ContextClassLoaderException(contextName);
      }
    }
    return new URLClassLoader(urls, ClassLoader.getSystemClassLoader());
  }

  @Override
  public ClassLoader getClassLoader(String contextName) throws ContextClassLoaderException {
    Path HDFSContextPath = new Path(System.getProperty(HDFS_CONTEXTS_BASE_DIR), contextName);
    try {
      URLClassLoader classLoader = classLoaders.getIfPresent(HDFSContextPath);
      if (classLoader != null) {
        return classLoader;
      }
      classLoader = createClassLoader(contextName, new Path(HDFSContextPath, MANIFEST_FILE_NAME));
      classLoaders.put(HDFSContextPath, classLoader);
      return classLoader;
    } catch (IOException e) {
      throw new ContextClassLoaderException(contextName, e);
    }
  }
}
