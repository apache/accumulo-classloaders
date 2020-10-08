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
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  private static List<WeakReference<DefaultFileSystemManager>> vfsInstances =
      Collections.synchronizedList(new ArrayList<>());
  private static volatile boolean DEBUG = false;

  static void enableDebug() {
    DEBUG = true;
  }

  static {
    // Register the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(new AccumuloVFSManagerShutdownThread()));
  }

  public static FileObject[] resolve(FileSystemManager vfs, String uris)
      throws FileSystemException {
    return resolve(vfs, uris, new ArrayList<>());
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
              if (DEBUG)
                System.out.println("classpath entry " + fo.getParent().toString() + " is "
                    + fo.getParent().getType().toString());
            }
          } else {
            if (DEBUG)
              System.out.println("ignoring classpath entry: " + fo.toString());
          }
          break;
        default:
          System.out.println("ignoring classpath entry:  " + fo.toString());
          break;
      }

    }

    return classpath.toArray(new FileObject[classpath.size()]);
  }

  public static DefaultFileSystemManager generateVfs() throws FileSystemException {
    DefaultFileSystemManager vfs = new DefaultFileSystemManager();
    vfs.addProvider("res", new org.apache.commons.vfs2.provider.res.ResourceFileProvider());
    vfs.addProvider("zip", new org.apache.commons.vfs2.provider.zip.ZipFileProvider());
    vfs.addProvider("gz", new org.apache.commons.vfs2.provider.gzip.GzipFileProvider());
    vfs.addProvider("ram", new org.apache.commons.vfs2.provider.ram.RamFileProvider());
    vfs.addProvider("file", new org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider());
    vfs.addProvider("jar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("http", new org.apache.commons.vfs2.provider.http.HttpFileProvider());
    vfs.addProvider("https", new org.apache.commons.vfs2.provider.https.HttpsFileProvider());
    vfs.addProvider("ftp", new org.apache.commons.vfs2.provider.ftp.FtpFileProvider());
    vfs.addProvider("ftps", new org.apache.commons.vfs2.provider.ftps.FtpsFileProvider());
    vfs.addProvider("war", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("par", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("ear", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("sar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("ejb3", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("tmp", new org.apache.commons.vfs2.provider.temp.TemporaryFileProvider());
    vfs.addProvider("tar", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
    vfs.addProvider("tbz2", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
    vfs.addProvider("tgz", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
    vfs.addProvider("bz2", new org.apache.commons.vfs2.provider.bzip2.Bzip2FileProvider());
    vfs.addProvider("hdfs", new HdfsFileProvider());
    vfs.addExtensionMap("jar", "jar");
    vfs.addExtensionMap("zip", "zip");
    vfs.addExtensionMap("gz", "gz");
    vfs.addExtensionMap("tar", "tar");
    vfs.addExtensionMap("tbz2", "tar");
    vfs.addExtensionMap("tgz", "tar");
    vfs.addExtensionMap("bz2", "bz2");
    vfs.addMimeTypeMap("application/x-tar", "tar");
    vfs.addMimeTypeMap("application/x-gzip", "gz");
    vfs.addMimeTypeMap("application/zip", "zip");
    vfs.setFileContentInfoFactory(new FileContentInfoFilenameFactory());
    vfs.setFilesCache(new SoftRefFilesCache());
    File cacheDir = computeTopCacheDir();
    vfs.setReplicator(new UniqueFileReplicator(cacheDir));
    vfs.setCacheStrategy(CacheStrategy.ON_RESOLVE);
    vfs.init();
    vfsInstances.add(new WeakReference<>(vfs));
    return vfs;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "tmpdir is controlled by admin, not unchecked user input")
  private static File computeTopCacheDir() {
    String cacheDirPath = AccumuloVFSClassLoader.getVFSCacheDir();
    String procName = ManagementFactory.getRuntimeMXBean().getName();
    return new File(cacheDirPath,
        "accumulo-vfs-manager-cache-" + procName + "-" + System.getProperty("user.name", "nouser"));
  }

  public static void returnVfs(DefaultFileSystemManager vfs) {
    if (DEBUG) {
      System.out.println("Closing VFS instance.");
    }
    FileReplicator replicator;
    try {
      replicator = vfs.getReplicator();
      if (replicator instanceof UniqueFileReplicator) {
        ((UniqueFileReplicator) replicator).close();
      }
    } catch (FileSystemException e) {
      System.err.println("Error occurred closing VFS instance: " + e.getMessage());
    }
    vfs.close();
  }

  public static void close() {
    for (WeakReference<DefaultFileSystemManager> vfsInstance : vfsInstances) {
      DefaultFileSystemManager ref = vfsInstance.get();
      if (ref != null) {
        returnVfs(ref);
      }
    }
    try {
      FileUtils.deleteDirectory(computeTopCacheDir());
    } catch (IOException e) {
      System.err.println("IOException deleting cache directory");
      e.printStackTrace();
    }
  }
}
