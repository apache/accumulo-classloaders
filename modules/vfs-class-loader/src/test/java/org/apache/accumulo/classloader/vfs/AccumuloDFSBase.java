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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.cache.DefaultFilesCache;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class AccumuloDFSBase {

  private static Configuration conf = null;
  private static DefaultFileSystemManager vfs = null;
  private static MiniDFSCluster cluster = null;

  private static URI HDFS_URI;

  protected static URI getHdfsUri() {
    return HDFS_URI;
  }

  public static String computeDatanodeDirectoryPermission() {
    // MiniDFSCluster will check the permissions on the data directories, but does not
    // do a good job of setting them properly. We need to get the users umask and set
    // the appropriate Hadoop property so that the data directories will be created
    // with the correct permissions.
    try {
      Process p = Runtime.getRuntime().exec("/bin/sh -c umask");
      try (BufferedReader bri =
          new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
        String line = bri.readLine();
        p.waitFor();

        if (line == null) {
          throw new IOException("umask input stream closed prematurely");
        }
        short umask = Short.parseShort(line.trim(), 8);
        // Need to set permission to 777 xor umask
        // leading zero makes java interpret as base 8
        int newPermission = 0777 ^ umask;

        return String.format("%03o", newPermission);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error getting umask from O/S", e);
    }
  }

  @BeforeClass
  public static void miniDfsClusterSetup() {
    System.setProperty("java.io.tmpdir", System.getProperty("user.dir") + "/target");

    // Put the MiniDFSCluster directory in the target directory
    System.setProperty("test.build.data", "target/build/test/data");

    // Setup HDFS
    conf = new Configuration();
    conf.set("hadoop.security.token.service.use_ip", "true");

    conf.set("dfs.datanode.data.dir.perm", computeDatanodeDirectoryPermission());
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024 * 1024); // 1M blocksize

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitClusterUp();
      // We can't assume that the hostname of "localhost" will still be "localhost" after
      // starting up the NameNode. We may get mapped into a FQDN via settings in /etc/hosts.
      HDFS_URI = cluster.getFileSystem().getUri();
    } catch (IOException e) {
      throw new RuntimeException("Error setting up mini cluster", e);
    }

    // Set up the VFS
    vfs = new DefaultFileSystemManager();
    try {
      vfs.setFilesCache(new DefaultFilesCache());
      vfs.addProvider("res", new org.apache.commons.vfs2.provider.res.ResourceFileProvider());
      vfs.addProvider("zip", new org.apache.commons.vfs2.provider.zip.ZipFileProvider());
      vfs.addProvider("gz", new org.apache.commons.vfs2.provider.gzip.GzipFileProvider());
      vfs.addProvider("ram", new org.apache.commons.vfs2.provider.ram.RamFileProvider());
      vfs.addProvider("file",
          new org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider());
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
      vfs.setReplicator(new DefaultFileReplicator(new File(System.getProperty("java.io.tmpdir"),
          "accumulo-vfs-cache-" + System.getProperty("user.name", "nouser"))));
      vfs.setCacheStrategy(CacheStrategy.ON_RESOLVE);
      vfs.init();
    } catch (FileSystemException e) {
      throw new RuntimeException("Error setting up VFS", e);
    }

  }

  public Configuration getConfiguration() {
    return conf;
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }

  public DefaultFileSystemManager getDefaultFileSystemManager() {
    return vfs;
  }

  @AfterClass
  public static void tearDownMiniDfsCluster() {
    if (null != cluster) {
      cluster.shutdown();
    }
  }

}
