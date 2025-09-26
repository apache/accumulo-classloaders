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
package org.apache.accumulo.classloader.vfs.context;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.accumulo.classloader.vfs.HDFSContextClassLoaderFactory;
import org.apache.accumulo.classloader.vfs.HDFSContextClassLoaderFactory.Context;
import org.apache.accumulo.classloader.vfs.HDFSContextClassLoaderFactory.JarInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class HDFSContextClassLoaderFactoryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(HDFSContextClassLoaderFactoryTest.class);
  private static final String CONTEXT1 = "context1";
  private static final String HELLO_WORLD_JAR = "HelloWorld.jar";
  private MiniDFSCluster hdfsCluster;
  private FileSystem fs;

  @BeforeEach
  public void setup() throws Exception {
    final String hdfsBaseDir = "/hdfs";
    final String hdfsContextsDir = hdfsBaseDir + "/contexts";
    final String hdfsContext1Dir = hdfsContextsDir + "/" + CONTEXT1;
    final String hdfsManifestFile =
        hdfsContext1Dir + "/" + HDFSContextClassLoaderFactory.MANIFEST_FILE_NAME;
    final String localContextsDir = System.getProperty("user.dir") + "/target/local/contexts";

    final JarInfo jarInfo1 = new JarInfo(HELLO_WORLD_JAR, "123abc");
    final Context context = new Context(CONTEXT1, jarInfo1);

    Configuration hadoopConf = new Configuration();
    hdfsCluster = new MiniDFSCluster.Builder(hadoopConf).build();
    hdfsCluster.waitClusterUp();
    fs = hdfsCluster.getFileSystem();

    Path HDFSContext1Path = new Path(hdfsContext1Dir);
    fs.mkdirs(HDFSContext1Path, FsPermission.getDirDefault());
    LOG.debug("Created dir(s) " + HDFSContext1Path);

    Path HDFSManifestPath = new Path(hdfsManifestFile);
    try (var os = fs.create(HDFSManifestPath)) {
      Gson gson = new Gson().newBuilder().setPrettyPrinting().create();
      os.write(gson.toJson(context).getBytes(StandardCharsets.UTF_8));
      LOG.debug("Wrote {}{}{} to {}", System.lineSeparator(), gson.toJson(context),
          System.lineSeparator(), HDFSManifestPath);
    }

    Path HDFSHelloWorldJar = new Path(HDFSContext1Path, HELLO_WORLD_JAR);
    Path localHelloWorldJar = new Path(this.getClass().getResource("/" + HELLO_WORLD_JAR).toURI());
    fs.copyFromLocalFile(localHelloWorldJar, HDFSHelloWorldJar);
    LOG.debug("Copied from {} to {}", localHelloWorldJar, HDFSHelloWorldJar);

    // set required system props for the factory
    System.setProperty(HDFSContextClassLoaderFactory.HDFS_CONTEXTS_BASE_DIR,
        fs.getUri() + hdfsContextsDir);
    System.setProperty(HDFSContextClassLoaderFactory.LOCAL_CONTEXTS_DOWNLOAD_DIR, localContextsDir);
  }

  @AfterEach
  public void teardown() throws Exception {
    hdfsCluster.close();
    hdfsCluster.shutdown(true);
  }

  @Test
  public void test() throws Exception {
    HDFSContextClassLoaderFactory factory = new HDFSContextClassLoaderFactory();
    factory.init(null);
    var classLoader = factory.getClassLoader(CONTEXT1);
    var clazz = classLoader.loadClass("test.HelloWorld");
    var methods = clazz.getMethods();
    assertEquals(1,
        Arrays.stream(methods).filter(method -> method.getName().equals("validate")).count());
  }
}
