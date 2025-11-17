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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LocalCachingContextClassLoaderTest {

  private static final String CONTEXT_NAME = "TEST_CONTEXT";
  private static final int MONITOR_INTERVAL_SECS = 5;
  private static MiniDFSCluster hdfs;
  private static Server jetty;
  private static ContextDefinition def;

  @TempDir
  private static java.nio.file.Path tempDir;

  @BeforeAll
  public static void beforeAll() throws Exception {
    String tmp = tempDir.resolve("base").toUri().toString();
    System.setProperty(Constants.CACHE_DIR_PROPERTY, tmp);

    // Find the Test jar files
    final URL jarAOrigLocation =
        LocalCachingContextClassLoaderTest.class.getResource("/ClassLoaderTestA/TestA.jar");
    assertNotNull(jarAOrigLocation);
    final URL jarBOrigLocation =
        LocalCachingContextClassLoaderTest.class.getResource("/ClassLoaderTestB/TestB.jar");
    assertNotNull(jarBOrigLocation);
    final URL jarCOrigLocation =
        LocalCachingContextClassLoaderTest.class.getResource("/ClassLoaderTestC/TestC.jar");
    assertNotNull(jarCOrigLocation);

    // Put B into HDFS
    hdfs = TestUtils.getMiniCluster();
    final FileSystem fs = hdfs.getFileSystem();
    assertTrue(fs.mkdirs(new Path("/contextB")));
    final Path dst = new Path("/contextB/TestB.jar");
    fs.copyFromLocalFile(new Path(jarBOrigLocation.toURI()), dst);
    assertTrue(fs.exists(dst));
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(hdfs.getConfiguration(0)));
    final URL jarBNewLocation = new URL(fs.getUri().toString() + dst.toUri().toString());

    // Put C into Jetty
    java.nio.file.Path jarCParentDirectory = Paths.get(jarCOrigLocation.toURI()).getParent();
    jetty = TestUtils.getJetty(jarCParentDirectory);
    final URL jarCNewLocation = jetty.getURI().resolve("TestC.jar").toURL();

    // Create ContextDefinition with all three resources
    final List<Resource> resources = new ArrayList<>();
    resources.add(new Resource(jarAOrigLocation.toString(),
        TestUtils.computeResourceChecksum(jarAOrigLocation)));
    resources.add(new Resource(jarBNewLocation.toString(),
        TestUtils.computeResourceChecksum(jarBOrigLocation)));
    resources.add(new Resource(jarCNewLocation.toString(),
        TestUtils.computeResourceChecksum(jarCOrigLocation)));

    def = new ContextDefinition(CONTEXT_NAME, MONITOR_INTERVAL_SECS, resources);
  }

  @AfterAll
  public static void afterAll() throws Exception {
    jetty.stop();
    jetty.join();
    hdfs.shutdown();
  }

  @Test
  public void testInitialize() throws ContextClassLoaderException, IOException {
    LocalCachingContextClassLoader lcccl = new LocalCachingContextClassLoader(def);
    lcccl.initialize();

    // Confirm the 3 jars are cached locally
    final java.nio.file.Path base = Paths.get(tempDir.resolve("base").toUri());
    assertTrue(Files.exists(base));
    assertTrue(Files.exists(base.resolve(CONTEXT_NAME)));
    for (Resource r : def.getResources()) {
      String filename = TestUtils.getFileName(r.getURL());
      String checksum = r.getChecksum();
      assertTrue(Files.exists(base.resolve(CONTEXT_NAME).resolve(filename + "_" + checksum)));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testClassLoader() throws Exception {

    LocalCachingContextClassLoader lcccl = new LocalCachingContextClassLoader(def);
    lcccl.initialize();
    ClassLoader contextClassLoader = lcccl.getClassloader();

    Class<? extends test.Test> clazzA =
        (Class<? extends test.Test>) contextClassLoader.loadClass("test.TestObjectA");
    test.Test a1 = clazzA.getDeclaredConstructor().newInstance();
    assertEquals("Hello from A", a1.hello());

    Class<? extends test.Test> clazzB =
        (Class<? extends test.Test>) contextClassLoader.loadClass("test.TestObjectB");
    test.Test b1 = clazzB.getDeclaredConstructor().newInstance();
    assertEquals("Hello from B", b1.hello());

    Class<? extends test.Test> clazzC =
        (Class<? extends test.Test>) contextClassLoader.loadClass("test.TestObjectC");
    test.Test c1 = clazzC.getDeclaredConstructor().newInstance();
    assertEquals("Hello from C", c1.hello());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUpdate() throws Exception {

    LocalCachingContextClassLoader lcccl = new LocalCachingContextClassLoader(def);
    lcccl.initialize();

    final ClassLoader contextClassLoader = lcccl.getClassloader();

    final Class<? extends test.Test> clazzA =
        (Class<? extends test.Test>) contextClassLoader.loadClass("test.TestObjectA");
    test.Test a1 = clazzA.getDeclaredConstructor().newInstance();
    assertEquals("Hello from A", a1.hello());

    final Class<? extends test.Test> clazzB =
        (Class<? extends test.Test>) contextClassLoader.loadClass("test.TestObjectB");
    test.Test b1 = clazzB.getDeclaredConstructor().newInstance();
    assertEquals("Hello from B", b1.hello());

    final Class<? extends test.Test> clazzC =
        (Class<? extends test.Test>) contextClassLoader.loadClass("test.TestObjectC");
    test.Test c1 = clazzC.getDeclaredConstructor().newInstance();
    assertEquals("Hello from C", c1.hello());

    List<Resource> updatedResources = new ArrayList<>(def.getResources());
    assertEquals(3, updatedResources.size());
    updatedResources.remove(2); // remove C

    // Add D
    final URL jarDOrigLocation =
        LocalCachingContextClassLoaderTest.class.getResource("/ClassLoaderTestD/TestD.jar");
    assertNotNull(jarDOrigLocation);
    updatedResources.add(new Resource(jarDOrigLocation.toString(),
        TestUtils.computeResourceChecksum(jarDOrigLocation)));

    ContextDefinition updatedDef =
        new ContextDefinition(CONTEXT_NAME, MONITOR_INTERVAL_SECS, updatedResources);
    lcccl.update(updatedDef);

    // Confirm the 3 jars are cached locally
    final java.nio.file.Path base = Paths.get(tempDir.resolve("base").toUri());
    assertTrue(Files.exists(base));
    assertTrue(Files.exists(base.resolve(CONTEXT_NAME)));
    for (Resource r : updatedDef.getResources()) {
      String filename = TestUtils.getFileName(r.getURL());
      assertFalse(filename.contains("C"));
      String checksum = r.getChecksum();
      assertTrue(Files.exists(base.resolve(CONTEXT_NAME).resolve(filename + "_" + checksum)));
    }

    final ClassLoader updatedContextClassLoader = lcccl.getClassloader();

    final Class<? extends test.Test> clazzA2 =
        (Class<? extends test.Test>) updatedContextClassLoader.loadClass("test.TestObjectA");
    test.Test a2 = clazzA2.getDeclaredConstructor().newInstance();
    assertNotEquals(clazzA, clazzA2);
    assertEquals("Hello from A", a2.hello());

    final Class<? extends test.Test> clazzB2 =
        (Class<? extends test.Test>) updatedContextClassLoader.loadClass("test.TestObjectB");
    test.Test b2 = clazzB2.getDeclaredConstructor().newInstance();
    assertNotEquals(clazzB, clazzB2);
    assertEquals("Hello from B", b2.hello());

    assertThrows(ClassNotFoundException.class,
        () -> updatedContextClassLoader.loadClass("test.TestObjectC"));

    final Class<? extends test.Test> clazzD =
        (Class<? extends test.Test>) updatedContextClassLoader.loadClass("test.TestObjectD");
    test.Test d1 = clazzD.getDeclaredConstructor().newInstance();
    assertEquals("Hello from D", d1.hello());

  }

}
