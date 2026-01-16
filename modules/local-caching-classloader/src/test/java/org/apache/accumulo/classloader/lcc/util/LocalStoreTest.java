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
package org.apache.accumulo.classloader.lcc.util;

import static org.apache.accumulo.classloader.lcc.TestUtils.testClassFailsToLoad;
import static org.apache.accumulo.classloader.lcc.TestUtils.testClassLoads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;

import org.apache.accumulo.classloader.lcc.TestUtils;
import org.apache.accumulo.classloader.lcc.TestUtils.TestClassInfo;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LocalStoreTest {

  @TempDir
  private static Path tempDir;

  private static final int MONITOR_INTERVAL_SECS = 5;
  private static MiniDFSCluster hdfs;
  private static Server jetty;
  private static ContextDefinition def;
  private static TestClassInfo classA;
  private static TestClassInfo classB;
  private static TestClassInfo classC;
  private static TestClassInfo classD;
  private static Path baseCacheDir = null;

  @BeforeAll
  public static void beforeAll() throws Exception {
    baseCacheDir = tempDir.resolve("base");

    // Find the Test jar files
    final URL jarAOrigLocation = LocalStoreTest.class.getResource("/ClassLoaderTestA/TestA.jar");
    assertNotNull(jarAOrigLocation);
    final URL jarBOrigLocation = LocalStoreTest.class.getResource("/ClassLoaderTestB/TestB.jar");
    assertNotNull(jarBOrigLocation);
    final URL jarCOrigLocation = LocalStoreTest.class.getResource("/ClassLoaderTestC/TestC.jar");
    assertNotNull(jarCOrigLocation);

    // Put B into HDFS
    hdfs = TestUtils.getMiniCluster();
    final FileSystem fs = hdfs.getFileSystem();
    assertTrue(fs.mkdirs(new org.apache.hadoop.fs.Path("/contextB")));
    final var dst = new org.apache.hadoop.fs.Path("/contextB/TestB.jar");
    fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(jarBOrigLocation.toURI()), dst);
    assertTrue(fs.exists(dst));
    URL.setURLStreamHandlerFactory(new HdfsUrlStreamHandlerFactory(hdfs.getConfiguration(0)));
    final URL jarBNewLocation = new URL(fs.getUri().toString() + dst.toUri().toString());

    // Put C into Jetty
    var jarCParentDirectory = Path.of(jarCOrigLocation.toURI()).getParent();
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

    def = new ContextDefinition(MONITOR_INTERVAL_SECS, resources);
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
        walker.map(Path::toFile).sorted(Comparator.reverseOrder()).forEach(File::delete);
      }
    }
  }

  @Test
  public void testPropertyNotSet() {
    assertThrows(NullPointerException.class, () -> new LocalStore(null));
  }

  @Test
  public void testCreateBaseDirs() throws Exception {
    assertFalse(Files.exists(baseCacheDir));
    var localStore = new LocalStore(baseCacheDir);
    assertTrue(Files.exists(baseCacheDir));
    assertTrue(Files.exists(baseCacheDir.resolve("contexts")));
    assertTrue(Files.exists(baseCacheDir.resolve("resources")));
    assertEquals(baseCacheDir.resolve("contexts"), localStore.contextsDir());
    assertEquals(baseCacheDir.resolve("resources"), localStore.resourcesDir());
  }

  @Test
  public void testCreateBaseDirsMultipleTimes() throws Exception {
    assertFalse(Files.exists(baseCacheDir));
    assertNotNull(new LocalStore(baseCacheDir));
    assertNotNull(new LocalStore(baseCacheDir));
    assertNotNull(new LocalStore(baseCacheDir));
    assertNotNull(new LocalStore(baseCacheDir));
    assertTrue(Files.exists(baseCacheDir));
  }

  @Test
  public void testLocalFileName() {
    // regular war
    assertEquals("f1-chk1.war", LocalStore.localResourceName("f1.war", "chk1"));
    // dotfile war
    assertEquals(".f1-chk1.war", LocalStore.localResourceName(".f1.war", "chk1"));
    // regular jar (has multiple dots)
    assertEquals("f2-1.0-chk2.jar", LocalStore.localResourceName("f2-1.0.jar", "chk2"));
    // dotfile jar (has multiple dots)
    assertEquals(".f2-1.0-chk2.jar", LocalStore.localResourceName(".f2-1.0.jar", "chk2"));
    // regular file with no suffix
    assertEquals("f3-chk3", LocalStore.localResourceName("f3", "chk3"));

    // weird files with trailing dots and no file suffix
    assertEquals("f4.-chk4", LocalStore.localResourceName("f4.", "chk4"));
    assertEquals("f4..-chk4", LocalStore.localResourceName("f4..", "chk4"));
    assertEquals("f4...-chk4", LocalStore.localResourceName("f4...", "chk4"));
    // weird dotfiles that don't really have a suffix
    assertEquals(".f5-chk5", LocalStore.localResourceName(".f5", "chk5"));
    assertEquals("..f5-chk5", LocalStore.localResourceName("..f5", "chk5"));
    // weird files with weird dots, but do have a valid suffix
    assertEquals("f6.-chk6.jar", LocalStore.localResourceName("f6..jar", "chk6"));
    assertEquals("f6..-chk6.jar", LocalStore.localResourceName("f6...jar", "chk6"));
    assertEquals(".f6-chk6.jar", LocalStore.localResourceName(".f6.jar", "chk6"));
    assertEquals("..f6-chk6.jar", LocalStore.localResourceName("..f6.jar", "chk6"));
    assertEquals(".f6.-chk6.jar", LocalStore.localResourceName(".f6..jar", "chk6"));
    assertEquals("..f6.-chk6.jar", LocalStore.localResourceName("..f6..jar", "chk6"));
  }

  @Test
  public void testStoreContextResources() throws Exception {
    var localStore = new LocalStore(baseCacheDir);
    localStore.storeContextResources(def);

    // Confirm the 3 jars are cached locally
    assertTrue(Files.exists(baseCacheDir));
    assertTrue(Files.exists(baseCacheDir.resolve("contexts").resolve(def.getChecksum() + ".json")));
    for (Resource r : def.getResources()) {
      String filename = TestUtils.getFileName(r.getLocation());
      String checksum = r.getChecksum();
      assertTrue(Files.exists(baseCacheDir.resolve("resources")
          .resolve(LocalStore.localResourceName(filename, checksum))));
    }
  }

  @Test
  public void testClassLoader() throws Exception {
    var urls = new LocalStore(baseCacheDir).storeContextResources(def);
    var contextClassLoader = LccUtils.createClassLoader("url", urls);

    testClassLoads(contextClassLoader, classA);
    testClassLoads(contextClassLoader, classB);
    testClassLoads(contextClassLoader, classC);
  }

  @Test
  public void testClassLoaderUpdate() throws Exception {
    var localStore = new LocalStore(baseCacheDir);
    var urls = localStore.storeContextResources(def);
    final var contextClassLoader = LccUtils.createClassLoader("url", urls);

    testClassLoads(contextClassLoader, classA);
    testClassLoads(contextClassLoader, classB);
    testClassLoads(contextClassLoader, classC);

    // keep all but C
    var updatedResources = def.getResources().stream().limit(def.getResources().size() - 1)
        .collect(Collectors.toCollection(LinkedHashSet::new));
    assertEquals(def.getResources().size() - 1, updatedResources.size());
    var removedResource = def.getResources().stream().reduce((a, b) -> b).orElseThrow();

    // Add D
    final URL jarDOrigLocation = LocalStoreTest.class.getResource("/ClassLoaderTestD/TestD.jar");
    assertNotNull(jarDOrigLocation);
    updatedResources
        .add(new Resource(jarDOrigLocation, TestUtils.computeResourceChecksum(jarDOrigLocation)));

    var updatedDef = new ContextDefinition(MONITOR_INTERVAL_SECS, updatedResources);
    urls = localStore.storeContextResources(updatedDef);

    // Confirm the 3 jars are cached locally
    assertTrue(
        Files.exists(baseCacheDir.resolve("contexts").resolve(updatedDef.getChecksum() + ".json")));
    for (Resource r : updatedDef.getResources()) {
      String filename = TestUtils.getFileName(r.getLocation());
      assertFalse(filename.contains("C"));
      String checksum = r.getChecksum();
      assertTrue(Files.exists(baseCacheDir.resolve("resources")
          .resolve(LocalStore.localResourceName(filename, checksum))));
    }

    String filename = TestUtils.getFileName(removedResource.getLocation());
    assertTrue(filename.contains("C"), "cache location should still contain 'C'");
    assertTrue(Files.exists(baseCacheDir.resolve("resources")
        .resolve(LocalStore.localResourceName(filename, removedResource.getChecksum()))));

    final var updatedContextClassLoader = LccUtils.createClassLoader("url", urls);

    assertNotEquals(contextClassLoader, updatedContextClassLoader);
    testClassLoads(updatedContextClassLoader, classA);
    testClassLoads(updatedContextClassLoader, classB);
    testClassFailsToLoad(updatedContextClassLoader, classC);
    testClassLoads(updatedContextClassLoader, classD);
  }

}
