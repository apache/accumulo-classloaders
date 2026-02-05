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
import static org.apache.accumulo.classloader.lcc.util.LccUtils.checksumForFileName;
import static org.apache.accumulo.classloader.lcc.util.LocalStore.CONTEXTS_DIR;
import static org.apache.accumulo.classloader.lcc.util.LocalStore.RESOURCES_DIR;
import static org.apache.accumulo.classloader.lcc.util.LocalStore.WORKING_DIR;
import static org.apache.accumulo.classloader.lcc.util.LocalStore.localResourceName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.accumulo.classloader.lcc.TestUtils;
import org.apache.accumulo.classloader.lcc.TestUtils.TestClassInfo;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

public class LocalStoreTest {

  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  private static Path tempDir;

  // a mock URL checker that allows all for test
  private static final BiConsumer<String,URL> ALLOW_ALL_URLS = (type, url) -> {};

  private static final int MONITOR_INTERVAL_SECS = 5;
  private static MiniDFSCluster hdfs;
  private static Server jetty;
  private static ContextDefinition def;
  private static TestClassInfo classA;
  private static TestClassInfo classB;
  private static TestClassInfo classC;
  private static TestClassInfo classD;
  private Path baseCacheDir;

  @BeforeAll
  public static void beforeAll() throws Exception {
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
    final URL jarBNewLocation = new URL(fs.getUri().toString() + dst.toUri().toString());

    // Put C into Jetty
    var jarCParentDirectory = Path.of(jarCOrigLocation.toURI()).getParent();
    jetty = TestUtils.getJetty(jarCParentDirectory);
    final URL jarCNewLocation = jetty.getURI().resolve("TestC.jar").toURL();

    // Create ContextDefinition with all three resources
    final LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    resources.add(new Resource(jarAOrigLocation, "SHA-256",
        TestUtils.computeResourceChecksum("SHA-256", jarAOrigLocation)));
    resources.add(new Resource(jarBNewLocation, "SHA-512",
        TestUtils.computeResourceChecksum("SHA-512", jarBOrigLocation)));
    resources.add(new Resource(jarCNewLocation, "SHA-1",
        TestUtils.computeResourceChecksum("SHA-1", jarCOrigLocation)));

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

  @BeforeEach
  public void createBaseDir(TestInfo info) {
    baseCacheDir = tempDir.resolve(info.getTestMethod().orElseThrow().getName());
  }

  @Test
  public void testPropertyNotSet() {
    // test baseDir not set
    assertThrows(NullPointerException.class, () -> new LocalStore(null, ALLOW_ALL_URLS));
    // test URL checker not set
    assertThrows(NullPointerException.class, () -> new LocalStore(baseCacheDir, null));
  }

  @Test
  public void testCreateBaseDirs() throws Exception {
    // these dirs are documented, so need to update docs when changing constant values
    assertEquals("resources", RESOURCES_DIR);
    assertEquals("working", WORKING_DIR);
    assertEquals("contexts", CONTEXTS_DIR);

    assertFalse(Files.exists(baseCacheDir));
    var localStore = new LocalStore(baseCacheDir, ALLOW_ALL_URLS);
    assertTrue(Files.exists(baseCacheDir));
    assertTrue(Files.exists(baseCacheDir.resolve(CONTEXTS_DIR)));
    assertTrue(Files.exists(baseCacheDir.resolve(RESOURCES_DIR)));
    assertTrue(Files.exists(baseCacheDir.resolve(WORKING_DIR)));
    assertEquals(baseCacheDir.resolve(CONTEXTS_DIR), localStore.contextsDir());
    assertEquals(baseCacheDir.resolve(RESOURCES_DIR), localStore.resourcesDir());
    assertEquals(baseCacheDir.resolve(WORKING_DIR), localStore.workingDir());
  }

  @Test
  public void testCreateBaseDirsMultipleTimes() throws Exception {
    assertFalse(Files.exists(baseCacheDir));
    assertNotNull(new LocalStore(baseCacheDir, ALLOW_ALL_URLS));
    assertNotNull(new LocalStore(baseCacheDir, ALLOW_ALL_URLS));
    assertNotNull(new LocalStore(baseCacheDir, ALLOW_ALL_URLS));
    assertNotNull(new LocalStore(baseCacheDir, ALLOW_ALL_URLS));
    assertTrue(Files.exists(baseCacheDir));
  }

  private static Resource rsrc(String filename, String algorithm, String checksum) {
    return new Resource() {
      @Override
      public String getFileName() {
        return filename;
      }

      @Override
      public String getAlgorithm() {
        return normalizeAlgorithm(algorithm);
      }

      @Override
      public String getChecksum() {
        return checksum;
      }
    };
  }

  @Test
  public void testLocalFileName() {
    // regular jar, test various algorithm name normalizations
    assertEquals("f0-MD5-chk0.jar", localResourceName(rsrc("f0.jar", "md5", "chk0")));
    assertEquals("f0-MD5-chk0.jar", localResourceName(rsrc("f0.jar", "MD5", "chk0")));
    assertEquals("f0-SHA-1-chk0.jar", localResourceName(rsrc("f0.jar", "sha1", "chk0")));
    assertEquals("f0-SHA-1-chk0.jar", localResourceName(rsrc("f0.jar", "Sha-1", "chk0")));
    assertEquals("f0-SHA-1-chk0.jar", localResourceName(rsrc("f0.jar", "SHA-1", "chk0")));
    assertEquals("f0-SHA-512-chk0.jar", localResourceName(rsrc("f0.jar", "sha512", "chk0")));
    assertEquals("f0-SHA-512_224-chk0.jar",
        localResourceName(rsrc("f0.jar", "sha512/224", "chk0")));
    assertEquals("f0-SHA3-224-chk0.jar", localResourceName(rsrc("f0.jar", "sha3-224", "chk0")));

    // regular war
    assertEquals("f1-mock-chk1.war", localResourceName(rsrc("f1.war", "mock", "chk1")));
    // dotfile war
    assertEquals(".f1-mock-chk1.war", localResourceName(rsrc(".f1.war", "mock", "chk1")));
    // regular jar (has multiple dots)
    assertEquals("f2-1.0-mock-chk2.jar", localResourceName(rsrc("f2-1.0.jar", "mock", "chk2")));
    // dotfile jar (has multiple dots)
    assertEquals(".f2-1.0-mock-chk2.jar", localResourceName(rsrc(".f2-1.0.jar", "mock", "chk2")));
    // regular file with no suffix
    assertEquals("f3-mock-chk3", localResourceName(rsrc("f3", "mock", "chk3")));

    // weird files with trailing dots and no file suffix
    assertEquals("f4.-mock-chk4", localResourceName(rsrc("f4.", "mock", "chk4")));
    assertEquals("f4..-mock-chk4", localResourceName(rsrc("f4..", "mock", "chk4")));
    assertEquals("f4...-mock-chk4", localResourceName(rsrc("f4...", "mock", "chk4")));
    // weird dotfiles that don't really have a suffix
    assertEquals(".f5-mock-chk5", localResourceName(rsrc(".f5", "mock", "chk5")));
    assertEquals("..f5-mock-chk5", localResourceName(rsrc("..f5", "mock", "chk5")));
    // weird files with weird dots, but do have a valid suffix
    assertEquals("f6.-mock-chk6.jar", localResourceName(rsrc("f6..jar", "mock", "chk6")));
    assertEquals("f6..-mock-chk6.jar", localResourceName(rsrc("f6...jar", "mock", "chk6")));
    assertEquals(".f6-mock-chk6.jar", localResourceName(rsrc(".f6.jar", "mock", "chk6")));
    assertEquals("..f6-mock-chk6.jar", localResourceName(rsrc("..f6.jar", "mock", "chk6")));
    assertEquals(".f6.-mock-chk6.jar", localResourceName(rsrc(".f6..jar", "mock", "chk6")));
    assertEquals("..f6.-mock-chk6.jar", localResourceName(rsrc("..f6..jar", "mock", "chk6")));
  }

  @Test
  public void testStoreContextResources() throws Exception {
    var localStore = new LocalStore(baseCacheDir, ALLOW_ALL_URLS);
    localStore.storeContextResources(def);

    // Confirm the 3 jars are cached locally
    assertTrue(Files.exists(baseCacheDir));
    assertTrue(Files
        .exists(baseCacheDir.resolve(CONTEXTS_DIR).resolve(checksumForFileName(def) + ".json")));
    for (Resource r : def.getResources()) {
      assertTrue(Files.exists(baseCacheDir.resolve(RESOURCES_DIR).resolve(localResourceName(r))));
    }
  }

  @Test
  public void testClassLoader() throws Exception {
    var localStore = new LocalStore(baseCacheDir, ALLOW_ALL_URLS);
    localStore.storeContextResources(def);
    var cacheKey = new ContextCacheKey("loc", def);
    var contextClassLoader = LccUtils.createClassLoader(cacheKey, localStore);

    testClassLoads(contextClassLoader, classA);
    testClassLoads(contextClassLoader, classB);
    testClassLoads(contextClassLoader, classC);
  }

  @Test
  public void testClassLoaderUpdate() throws Exception {
    var localStore = new LocalStore(baseCacheDir, ALLOW_ALL_URLS);
    localStore.storeContextResources(def);
    var cacheKey = new ContextCacheKey("loc", def);
    final var contextClassLoader = LccUtils.createClassLoader(cacheKey, localStore);

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
    updatedResources.add(new Resource(jarDOrigLocation, "SHA-512",
        TestUtils.computeResourceChecksum("SHA-512", jarDOrigLocation)));

    var updatedDef = new ContextDefinition(MONITOR_INTERVAL_SECS, updatedResources);
    localStore.storeContextResources(updatedDef);

    // Confirm the 3 jars are cached locally
    assertTrue(Files.exists(
        baseCacheDir.resolve(CONTEXTS_DIR).resolve(checksumForFileName(updatedDef) + ".json")));
    for (Resource r : updatedDef.getResources()) {
      assertFalse(r.getFileName().contains("C"));
      assertTrue(Files.exists(baseCacheDir.resolve(RESOURCES_DIR).resolve(localResourceName(r))));
    }

    assertTrue(removedResource.getFileName().contains("C"),
        "cache location should still contain 'C'");
    assertTrue(Files
        .exists(baseCacheDir.resolve(RESOURCES_DIR).resolve(localResourceName(removedResource))));

    cacheKey = new ContextCacheKey("loc", updatedDef);
    final var updatedContextClassLoader = LccUtils.createClassLoader(cacheKey, localStore);

    assertNotEquals(contextClassLoader, updatedContextClassLoader);
    testClassLoads(updatedContextClassLoader, classA);
    testClassLoads(updatedContextClassLoader, classB);
    testClassFailsToLoad(updatedContextClassLoader, classC);
    testClassLoads(updatedContextClassLoader, classD);
  }

  @Test
  public void testClassLoaderDirCleanup() throws Exception {
    var localStore = new LocalStore(baseCacheDir, ALLOW_ALL_URLS);
    assertEquals(0, Files.list(localStore.workingDir()).filter(Files::isDirectory).count());
    localStore.storeContextResources(def);
    var cacheKey = new ContextCacheKey("loc", def);
    assertEquals(0, Files.list(localStore.workingDir()).filter(Files::isDirectory).count());

    final var endBackgroundThread = new CountDownLatch(1);
    var createdClassLoader = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      final var contextClassLoader = LccUtils.createClassLoader(cacheKey, localStore);
      createdClassLoader.countDown();
      try {
        testClassLoads(contextClassLoader, classA);
        endBackgroundThread.await();
      } catch (Exception e) {
        fail(e);
      }
      // hold a strong reference in the background thread long enough to prevent cleanup
      assertNotNull(contextClassLoader);
    });
    t.start();
    createdClassLoader.await();

    assertEquals(1, Files.list(localStore.workingDir()).filter(Files::isDirectory).count());

    endBackgroundThread.countDown();

    // wait for classloader to be garbage collected and its cleaner to run
    while (Files.list(localStore.workingDir()).filter(Files::isDirectory).count() != 0) {
      System.gc();
      Thread.sleep(50);
    }
  }
}
