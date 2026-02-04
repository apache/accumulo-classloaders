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
package org.apache.accumulo.classloader.lcc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.classloader.lcc.util.LccUtils.getDigester;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.PathResource;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TestUtils {

  public static class TestClassInfo {
    private final String className;
    private final String helloOutput;

    public TestClassInfo(String className, String helloOutput) {
      super();
      this.className = className;
      this.helloOutput = helloOutput;
    }

    public String getClassName() {
      return className;
    }

    public String getHelloOutput() {
      return helloOutput;
    }
  }

  public static org.apache.hadoop.fs.Path createContextDefinitionFile(FileSystem fs, String name,
      String contents) throws Exception {
    var baseHdfsPath = new org.apache.hadoop.fs.Path("/contextDefs");
    assertTrue(fs.mkdirs(baseHdfsPath));
    var newContextDefinitionFile = new org.apache.hadoop.fs.Path(baseHdfsPath, name);

    if (contents == null) {
      assertTrue(fs.createNewFile(newContextDefinitionFile));
    } else {
      try (FSDataOutputStream out = fs.create(newContextDefinitionFile)) {
        out.writeBytes(contents);
      }
    }
    assertTrue(fs.exists(newContextDefinitionFile));
    return newContextDefinitionFile;
  }

  public static void updateContextDefinitionFile(FileSystem fs,
      org.apache.hadoop.fs.Path defFilePath, String contents) throws Exception {
    // Update the contents of the context definition json file
    assertTrue(fs.exists(defFilePath));
    fs.delete(defFilePath, false);
    assertFalse(fs.exists(defFilePath));

    if (contents == null) {
      assertTrue(fs.createNewFile(defFilePath));
    } else {
      try (FSDataOutputStream out = fs.create(defFilePath)) {
        out.writeBytes(contents);
      }
    }
    assertTrue(fs.exists(defFilePath));

  }

  public static void testClassLoads(ClassLoader cl, TestClassInfo tci) throws Exception {
    var clazz = cl.loadClass(tci.getClassName()).asSubclass(test.Test.class);
    test.Test impl = clazz.getDeclaredConstructor().newInstance();
    assertEquals(tci.getHelloOutput(), impl.hello());
  }

  public static void testClassFailsToLoad(ClassLoader cl, TestClassInfo tci) throws Exception {
    assertThrows(ClassNotFoundException.class, () -> cl.loadClass(tci.getClassName()));
  }

  private static String computeDatanodeDirectoryPermission() {
    // MiniDFSCluster will check the permissions on the data directories, but does not
    // do a good job of setting them properly. We need to get the users umask and set
    // the appropriate Hadoop property so that the data directories will be created
    // with the correct permissions.
    try {
      Process p = Runtime.getRuntime().exec("/bin/sh -c umask");
      try (BufferedReader bri =
          new BufferedReader(new InputStreamReader(p.getInputStream(), UTF_8))) {
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

  public static MiniDFSCluster getMiniCluster() throws IOException {
    System.setProperty("java.io.tmpdir", System.getProperty("user.dir") + "/target");

    // Put the MiniDFSCluster directory in the target directory
    System.setProperty("test.build.data", "target/build/test/data");

    // Setup HDFS
    Configuration conf = new Configuration();
    conf.set("hadoop.security.token.service.use_ip", "true");

    conf.set("dfs.datanode.data.dir.perm", computeDatanodeDirectoryPermission());
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024 * 1024); // 1M blocksize

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitClusterUp();
    return cluster;
  }

  public static Server getJetty(Path resourceDirectory) throws Exception {
    PathResource directory = new PathResource(resourceDirectory);
    ResourceHandler handler = new ResourceHandler();
    handler.setBaseResource(directory);

    Server jetty = new Server(0);
    jetty.setHandler(handler);
    jetty.start();
    return jetty;
  }

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "user-supplied URL is the intended functionality")
  public static String computeResourceChecksum(String algorithm, URL resourceLocation)
      throws IOException {
    try (InputStream is = resourceLocation.openStream()) {
      return getDigester(algorithm).digestAsHex(is);
    }
  }

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "user-supplied URL is the intended functionality")
  public static long getFileSize(URL url) throws IOException {
    try (InputStream is = url.openStream()) {
      return IOUtils.consume(is);
    }
  }

  public static long getFileSize(Path p) throws IOException {
    try (InputStream is = Files.newInputStream(p)) {
      return IOUtils.consume(is);
    }
  }

}
