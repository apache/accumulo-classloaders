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
package org.apache.accumulo.classloader.lcc.resolvers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.accumulo.classloader.lcc.TestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileResolversTest {

  private static final Logger LOG = LoggerFactory.getLogger(FileResolversTest.class);

  private long getFileSize(Path p) throws IOException {
    try (InputStream is = Files.newInputStream(p, StandardOpenOption.READ)) {
      return IOUtils.consume(is);
    }
  }

  private long getFileSize(FileResolver resolver) throws IOException {
    try (InputStream is = resolver.getInputStream()) {
      return IOUtils.consume(is);
    }
  }

  @Test
  public void testLocalFile() throws Exception {
    URI jarPath = FileResolversTest.class.getResource("/HelloWorld.jar").toURI();
    assertNotNull(jarPath);
    var p = Path.of(jarPath);
    final long origFileSize = getFileSize(p);
    FileResolver resolver = FileResolver.resolve(jarPath);
    assertTrue(resolver instanceof LocalFileResolver);
    assertEquals(jarPath, resolver.getURI());
    assertEquals("HelloWorld.jar", resolver.getFileName());
    assertEquals(origFileSize, getFileSize(resolver));
  }

  @Test
  public void testHttpFile() throws Exception {

    URI jarPath = FileResolversTest.class.getResource("/HelloWorld.jar").toURI();
    assertNotNull(jarPath);
    var p = Path.of(jarPath);
    final long origFileSize = getFileSize(p);

    Server jetty = TestUtils.getJetty(p.getParent());
    LOG.debug("Jetty listening at: {}", jetty.getURI());
    URI httpPath = jetty.getURI().resolve("HelloWorld.jar");
    FileResolver resolver = FileResolver.resolve(httpPath);
    assertTrue(resolver instanceof HttpFileResolver);
    assertEquals(httpPath, resolver.getURI());
    assertEquals("HelloWorld.jar", resolver.getFileName());
    assertEquals(origFileSize, getFileSize(resolver));

    jetty.stop();
    jetty.join();
  }

  @Test
  public void testHdfsFile() throws Exception {

    URI jarPath = FileResolversTest.class.getResource("/HelloWorld.jar").toURI();
    assertNotNull(jarPath);
    var p = Path.of(jarPath);
    final long origFileSize = getFileSize(p);

    MiniDFSCluster cluster = TestUtils.getMiniCluster();
    try {
      FileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdirs(new org.apache.hadoop.fs.Path("/context1")));
      var dst = new org.apache.hadoop.fs.Path("/context1/HelloWorld.jar");
      fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(jarPath), dst);
      assertTrue(fs.exists(dst));

      URI fullPath = new URI(fs.getUri().toString() + dst.toUri().toString());
      LOG.info("Path to hdfs file: {}", fullPath);

      FileResolver resolver = FileResolver.resolve(fullPath);
      assertTrue(resolver instanceof HdfsFileResolver);
      assertEquals(fullPath, resolver.getURI());
      assertEquals("HelloWorld.jar", resolver.getFileName());
      assertEquals(origFileSize, getFileSize(resolver));

    } catch (IOException e) {
      throw new RuntimeException("Error setting up mini cluster", e);
    } finally {
      cluster.shutdown();
    }
  }

}
