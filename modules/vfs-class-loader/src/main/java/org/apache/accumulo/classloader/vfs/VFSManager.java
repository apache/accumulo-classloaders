/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.classloader.vfs;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.provider.FileReplicator;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileProvider;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class VFSManager {

  public static class AccumuloVFSManagerShutdownThread implements Runnable {

    @Override
    public void run() {
      try {
        VFSManager.close();
      } catch (Exception e) {
        // do nothing, we are shutting down anyway
      }
    }
  }

  private static DefaultFileSystemManager VFS = null;
  private static volatile boolean DEBUG = false;

  static {
    // Register the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(new AccumuloVFSManagerShutdownThread()));
  }

  public static void enableDebug() {
    DEBUG = true;
  }

  private static void printDebug(String msg) {
    if (!DEBUG)
      return;
    System.out.println(String.format("DEBUG: %d VFSManager: %s", System.currentTimeMillis(), msg));
  }

  private static void printError(String msg) {
    System.err.println(String.format("ERROR: %d VFSManager: %s", System.currentTimeMillis(), msg));
  }

  public static FileObject[] resolve(String uris) throws FileSystemException {
    return resolve(VFS, uris, new ArrayList<>());
  }

  static FileObject[] resolve(FileSystemManager vfs, String uris,
      ArrayList<FileObject> pathsToMonitor) throws FileSystemException {
    if (uris == null) {
      return new FileObject[0];
    }

    ArrayList<FileObject> classpath = new ArrayList<>();

    pathsToMonitor.clear();

    for (String path : uris.split(",")) {

      path = path.trim();

      if (path.equals("")) {
        continue;
      }

      path = AccumuloVFSClassLoader.replaceEnvVars(path, System.getenv());

      printDebug("Resolving path element: " + path);
      FileObject fo = vfs.resolveFile(path);

      switch (fo.getType()) {
        case FILE:
        case FOLDER:
          classpath.add(fo);
          pathsToMonitor.add(fo);
          break;
        case IMAGINARY:
          // assume its a pattern
          String pattern = fo.getName().getBaseName();
          if (fo.getParent() != null) {
            // still monitor the parent
            pathsToMonitor.add(fo.getParent());
            if (fo.getParent().getType() == FileType.FOLDER) {
              FileObject[] children = fo.getParent().getChildren();
              for (FileObject child : children) {
                if (child.getType() == FileType.FILE
                    && child.getName().getBaseName().matches(pattern)) {
                  classpath.add(child);
                }
              }
            } else {
              if (DEBUG) {
                printDebug("classpath entry " + fo.getParent().toString() + " is "
                    + fo.getParent().getType().toString());
              }
            }
          } else {
            if (DEBUG) {
              printDebug("ignoring classpath entry: " + fo.toString());
            }
          }
          break;
        default:
          if (DEBUG) {
            printDebug("ignoring classpath entry:  " + fo.toString());
          }
          break;
      }

    }

    return classpath.toArray(new FileObject[classpath.size()]);
  }

  public static void initialize() throws FileSystemException {
    if (null == VFS) {
      VFS = new DefaultFileSystemManager();
      VFS.addProvider("res", new org.apache.commons.vfs2.provider.res.ResourceFileProvider());
      VFS.addProvider("zip", new org.apache.commons.vfs2.provider.zip.ZipFileProvider());
      VFS.addProvider("gz", new org.apache.commons.vfs2.provider.gzip.GzipFileProvider());
      VFS.addProvider("ram", new org.apache.commons.vfs2.provider.ram.RamFileProvider());
      VFS.addProvider("file",
          new org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider());
      VFS.addProvider("jar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      VFS.addProvider("http", new org.apache.commons.vfs2.provider.http.HttpFileProvider());
      VFS.addProvider("https", new org.apache.commons.vfs2.provider.https.HttpsFileProvider());
      VFS.addProvider("ftp", new org.apache.commons.vfs2.provider.ftp.FtpFileProvider());
      VFS.addProvider("ftps", new org.apache.commons.vfs2.provider.ftps.FtpsFileProvider());
      VFS.addProvider("war", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      VFS.addProvider("par", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      VFS.addProvider("ear", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      VFS.addProvider("sar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      VFS.addProvider("ejb3", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      VFS.addProvider("tmp", new org.apache.commons.vfs2.provider.temp.TemporaryFileProvider());
      VFS.addProvider("tar", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      VFS.addProvider("tbz2", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      VFS.addProvider("tgz", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      VFS.addProvider("bz2", new org.apache.commons.vfs2.provider.bzip2.Bzip2FileProvider());
      VFS.addProvider("hdfs", new HdfsFileProvider());
      VFS.addExtensionMap("jar", "jar");
      VFS.addExtensionMap("zip", "zip");
      VFS.addExtensionMap("gz", "gz");
      VFS.addExtensionMap("tar", "tar");
      VFS.addExtensionMap("tbz2", "tar");
      VFS.addExtensionMap("tgz", "tar");
      VFS.addExtensionMap("bz2", "bz2");
      VFS.addMimeTypeMap("application/x-tar", "tar");
      VFS.addMimeTypeMap("application/x-gzip", "gz");
      VFS.addMimeTypeMap("application/zip", "zip");
      VFS.setFileContentInfoFactory(new FileContentInfoFilenameFactory());
      VFS.setFilesCache(new SoftRefFilesCache());
      File cacheDir = computeTopCacheDir();
      VFS.setReplicator(new UniqueFileReplicator(cacheDir));
      VFS.setCacheStrategy(CacheStrategy.ON_RESOLVE);
      VFS.init();
    }
  }

  public static FileSystemManager get() {
    return VFS;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "tmpdir is controlled by admin, not unchecked user input")
  private static File computeTopCacheDir() {
    String cacheDirPath = AccumuloVFSClassLoader.getVFSCacheDir();
    String procName = ManagementFactory.getRuntimeMXBean().getName();
    return new File(cacheDirPath,
        "accumulo-vfs-manager-cache-" + procName + "-" + System.getProperty("user.name", "nouser"));
  }

  private static void close() {
    printDebug("Closing VFS instance.");
    FileReplicator replicator;
    try {
      replicator = VFS.getReplicator();
      if (replicator instanceof UniqueFileReplicator) {
        ((UniqueFileReplicator) replicator).close();
      }
    } catch (FileSystemException e) {
      printError("Error occurred closing VFS instance: " + e.getMessage());
    }
    VFS.close();
    try {
      FileUtils.deleteDirectory(computeTopCacheDir());
    } catch (IOException e) {
      printError("IOException deleting cache directory");
      e.printStackTrace();
    }
  }
}
