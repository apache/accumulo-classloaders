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
package org.apache.accumulo.classloader.cargo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a variety of URL types. Specifically, test file:, http:, and hdfs: types.
 */
public class URLTypesTest {

  private static final Logger LOG = LoggerFactory.getLogger(URLTypesTest.class);

  @Test
  public void testLocalFile() throws Exception {
    URL jarPath = URLTypesTest.class.getResource("/HelloWorld.jar");
    assertNotNull(jarPath);
    var p = Path.of(jarPath.toURI());
    final long origFileSize = TestUtils.getFileSize(p);
    assertEquals(origFileSize, TestUtils.getFileSize(jarPath));
  }

  @Test
  public void testHttpFile() throws Exception {

    URL jarPath = URLTypesTest.class.getResource("/HelloWorld.jar");
    assertNotNull(jarPath);
    var p = Path.of(jarPath.toURI());
    final long origFileSize = TestUtils.getFileSize(p);

    Server jetty = TestUtils.getJetty(p.getParent());
    LOG.debug("Jetty listening at: {}", jetty.getURI());
    URL httpPath = jetty.getURI().resolve("HelloWorld.jar").toURL();
    assertEquals(origFileSize, TestUtils.getFileSize(httpPath));

    jetty.stop();
    jetty.join();
  }

  @Test
  public void testHdfsFile() throws Exception {

    URL jarPath = URLTypesTest.class.getResource("/HelloWorld.jar");
    assertNotNull(jarPath);
    var p = Path.of(jarPath.toURI());
    final long origFileSize = TestUtils.getFileSize(p);

    MiniDFSCluster cluster = TestUtils.getMiniCluster();
    try {
      FileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdirs(new org.apache.hadoop.fs.Path("/context1")));
      var dst = new org.apache.hadoop.fs.Path("/context1/HelloWorld.jar");
      fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(jarPath.toURI()), dst);
      assertTrue(fs.exists(dst));

      URL fullPath = new URL(fs.getUri().toString() + dst.toUri().toString());
      LOG.info("Path to hdfs file: {}", fullPath);

      assertEquals(origFileSize, TestUtils.getFileSize(fullPath));

    } catch (IOException e) {
      throw new RuntimeException("Error setting up mini cluster", e);
    } finally {
      cluster.shutdown();
    }
  }

}
