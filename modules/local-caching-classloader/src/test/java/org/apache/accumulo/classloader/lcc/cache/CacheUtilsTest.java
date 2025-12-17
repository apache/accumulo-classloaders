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
package org.apache.accumulo.classloader.lcc.cache;

import static org.apache.accumulo.classloader.lcc.TestUtils.testClassFailsToLoad;
import static org.apache.accumulo.classloader.lcc.TestUtils.testClassLoads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.accumulo.classloader.lcc.TestUtils;
import org.apache.accumulo.classloader.lcc.TestUtils.TestClassInfo;
import org.apache.accumulo.classloader.lcc.cache.CacheUtils.LockInfo;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CacheUtilsTest {

  @TempDir
  private static java.nio.file.Path tempDir;

  private static final String CONTEXT_NAME = "TEST_CONTEXT";
  private static final int MONITOR_INTERVAL_SECS = 5;
  private static MiniDFSCluster hdfs;
  private static Server jetty;
  private static ContextDefinition def;
  private static TestClassInfo classA;
  private static TestClassInfo classB;
  private static TestClassInfo classC;
  private static TestClassInfo classD;
  private static java.nio.file.Path baseCacheDir = null;

  @BeforeAll
  public static void beforeAll() throws Exception {
    baseCacheDir = tempDir.resolve("base");

    // Find the Test jar files
    final URL jarAOrigLocation = CacheUtilsTest.class.getResource("/ClassLoaderTestA/TestA.jar");
    assertNotNull(jarAOrigLocation);
    final URL jarBOrigLocation = CacheUtilsTest.class.getResource("/ClassLoaderTestB/TestB.jar");
    assertNotNull(jarBOrigLocation);
    final URL jarCOrigLocation = CacheUtilsTest.class.getResource("/ClassLoaderTestC/TestC.jar");
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
    java.nio.file.Path jarCParentDirectory =
        java.nio.file.Path.of(jarCOrigLocation.toURI()).getParent();
    jetty = TestUtils.getJetty(jarCParentDirectory);
    final URL jarCNewLocation = jetty.getURI().resolve("TestC.jar").toURL();

    // Create ContextDefinition with all three resources
    final LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    resources
        .add(new Resource(jarAOrigLocation, TestUtils.computeResourceChecksum(jarAOrigLocation)));
    resources
        .add(new Resource(jarBNewLocation, TestUtils.computeResourceChecksum(jarBOrigLocation)));
    resources
        .add(new Resource(jarCNewLocation, TestUtils.computeResourceChecksum(jarCOrigLocation)));

    def = new ContextDefinition(CONTEXT_NAME, MONITOR_INTERVAL_SECS, resources);
    classA = new TestClassInfo("test.TestObjectA", "Hello from A");
    classB = new TestClassInfo("test.TestObjectB", "Hello from B");
    classC = new TestClassInfo("test.TestObjectC", "Hello from C");
    classD = new TestClassInfo("test.TestObjectD", "Hello from D");
  }

  @AfterAll
  public static void afterAll() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty.join();
    }
    if (hdfs != null) {
      hdfs.shutdown();
    }
  }

  @AfterEach
  public void cleanBaseDir() throws Exception {
    if (Files.exists(baseCacheDir)) {
      try (var walker = Files.walk(baseCacheDir)) {
        walker.map(java.nio.file.Path::toFile).sorted(Comparator.reverseOrder())
            .forEach(File::delete);
      }
    }
  }

  @Test
  public void testPropertyNotSet() {
    var ex = assertThrows(ContextClassLoaderException.class, () -> CacheUtils.createBaseDirs(null));
    assertEquals("Error getting classloader for context: received null for cache directory",
        ex.getMessage());
  }

  @Test
  public void testCreateBaseDirs() throws Exception {
    assertFalse(Files.exists(baseCacheDir));
    CacheUtils.createBaseDirs(baseCacheDir);
    assertTrue(Files.exists(baseCacheDir));
    assertTrue(Files.exists(baseCacheDir.resolve("contexts")));
    assertTrue(Files.exists(baseCacheDir.resolve("resources")));
    assertEquals(baseCacheDir.resolve("contexts"), CacheUtils.contextsDir(baseCacheDir));
    assertEquals(baseCacheDir.resolve("resources"), CacheUtils.resourcesDir(baseCacheDir));
  }

  @Test
  public void testCreateBaseDirsMultipleTimes() throws Exception {
    assertFalse(Files.exists(baseCacheDir));
    CacheUtils.createBaseDirs(baseCacheDir);
    CacheUtils.createBaseDirs(baseCacheDir);
    CacheUtils.createBaseDirs(baseCacheDir);
    CacheUtils.createBaseDirs(baseCacheDir);
    assertTrue(Files.exists(baseCacheDir));
  }

  @Test
  public void testLock() throws Exception {
    final java.nio.file.Path contextsDir = baseCacheDir.resolve("contexts");
    try {
      assertFalse(Files.exists(baseCacheDir));
      CacheUtils.createBaseDirs(baseCacheDir);
      assertTrue(Files.exists(baseCacheDir));
      assertTrue(Files.exists(contextsDir));

      final LockInfo lockInfo = CacheUtils.lockContextCacheDir(contextsDir);
      try {
        assertNotNull(lockInfo);
        assertTrue(lockInfo.getLock().acquiredBy().equals(lockInfo.getChannel()));
        assertFalse(lockInfo.getLock().isShared());
        assertTrue(lockInfo.getLock().isValid());

        // Test that another thread can't get the lock
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread t = new Thread(() -> {
          try {
            assertNull(CacheUtils.lockContextCacheDir(contextsDir));
          } catch (ContextClassLoaderException e) {
            error.set(e);
          }
        });
        t.start();
        t.join();
        assertNull(error.get());

      } finally {
        lockInfo.unlock();
      }
    } finally {
      Files.delete(contextsDir.resolve("lock_file"));
    }

  }

  @Test
  public void testInitialize() throws Exception {
    CacheUtils.stageContext(baseCacheDir, def);

    // Confirm the 3 jars are cached locally
    assertTrue(Files.exists(baseCacheDir));
    assertTrue(Files
        .exists(baseCacheDir.resolve("contexts").resolve(CONTEXT_NAME + "_" + def.getChecksum())));
    for (Resource r : def.getResources()) {
      String filename = TestUtils.getFileName(r.getLocation());
      String checksum = r.getChecksum();
      assertTrue(
          Files.exists(baseCacheDir.resolve("resources").resolve(filename + "_" + checksum)));
    }
  }

  @Test
  public void testClassLoader() throws Exception {
    var helper = CacheUtils.stageContext(baseCacheDir, def);
    ClassLoader contextClassLoader = helper.createClassLoader();

    testClassLoads(contextClassLoader, classA);
    testClassLoads(contextClassLoader, classB);
    testClassLoads(contextClassLoader, classC);
  }

  @Test
  public void testUpdate() throws Exception {
    var helper = CacheUtils.stageContext(baseCacheDir, def);
    final ClassLoader contextClassLoader = helper.createClassLoader();

    testClassLoads(contextClassLoader, classA);
    testClassLoads(contextClassLoader, classB);
    testClassLoads(contextClassLoader, classC);

    // keep all but C
    var updatedResources = def.getResources().stream().limit(def.getResources().size() - 1)
        .collect(Collectors.toCollection(LinkedHashSet::new));
    assertEquals(def.getResources().size() - 1, updatedResources.size());

    // Add D
    final URL jarDOrigLocation = CacheUtilsTest.class.getResource("/ClassLoaderTestD/TestD.jar");
    assertNotNull(jarDOrigLocation);
    updatedResources
        .add(new Resource(jarDOrigLocation, TestUtils.computeResourceChecksum(jarDOrigLocation)));

    var updatedDef = new ContextDefinition(CONTEXT_NAME, MONITOR_INTERVAL_SECS, updatedResources);
    helper = CacheUtils.stageContext(baseCacheDir, updatedDef);

    // Confirm the 3 jars are cached locally
    assertTrue(Files.exists(
        baseCacheDir.resolve("contexts").resolve(CONTEXT_NAME + "_" + updatedDef.getChecksum())));
    for (Resource r : updatedDef.getResources()) {
      String filename = TestUtils.getFileName(r.getLocation());
      assertFalse(filename.contains("C"));
      String checksum = r.getChecksum();
      assertTrue(
          Files.exists(baseCacheDir.resolve("resources").resolve(filename + "_" + checksum)));
    }

    final ClassLoader updatedContextClassLoader = helper.createClassLoader();

    assertNotEquals(contextClassLoader, updatedContextClassLoader);
    testClassLoads(updatedContextClassLoader, classA);
    testClassLoads(updatedContextClassLoader, classB);
    testClassFailsToLoad(updatedContextClassLoader, classC);
    testClassLoads(updatedContextClassLoader, classD);
  }

}
