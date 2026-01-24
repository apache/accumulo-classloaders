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

import static org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory.CACHE_DIR_PROPERTY;
import static org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory.UPDATE_FAILURE_GRACE_PERIOD_MINS;
import static org.apache.accumulo.classloader.lcc.TestUtils.createContextDefinitionFile;
import static org.apache.accumulo.classloader.lcc.TestUtils.testClassFailsToLoad;
import static org.apache.accumulo.classloader.lcc.TestUtils.testClassLoads;
import static org.apache.accumulo.classloader.lcc.TestUtils.updateContextDefinitionFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.accumulo.classloader.lcc.TestUtils.TestClassInfo;
import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.util.LocalStore;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.JsonSyntaxException;

public class LocalCachingContextClassLoaderFactoryTest {

  protected static final int MONITOR_INTERVAL_SECS = 5;
  private static MiniDFSCluster hdfs;
  private static FileSystem fs;
  private static Server jetty;
  private static URL jarAOrigLocation;
  private static URL jarBOrigLocation;
  private static URL jarCOrigLocation;
  private static URL jarDOrigLocation;
  private static URL jarEOrigLocation;
  private static URL localAllContext;
  private static URL hdfsAllContext;
  private static URL jettyAllContext;
  private static TestClassInfo classA;
  private static TestClassInfo classB;
  private static TestClassInfo classC;
  private static TestClassInfo classD;

  private LocalCachingContextClassLoaderFactory FACTORY;
  private Path baseCacheDir;

  @TempDir
  private Path tempDir;

  @BeforeAll
  public static void beforeAll() throws Exception {
    // Find the Test jar files
    jarAOrigLocation =
        LocalCachingContextClassLoaderFactoryTest.class.getResource("/ClassLoaderTestA/TestA.jar");
    assertNotNull(jarAOrigLocation);
    jarBOrigLocation =
        LocalCachingContextClassLoaderFactoryTest.class.getResource("/ClassLoaderTestB/TestB.jar");
    assertNotNull(jarBOrigLocation);
    jarCOrigLocation =
        LocalCachingContextClassLoaderFactoryTest.class.getResource("/ClassLoaderTestC/TestC.jar");
    assertNotNull(jarCOrigLocation);
    jarDOrigLocation =
        LocalCachingContextClassLoaderFactoryTest.class.getResource("/ClassLoaderTestD/TestD.jar");
    assertNotNull(jarDOrigLocation);
    jarEOrigLocation =
        LocalCachingContextClassLoaderFactoryTest.class.getResource("/ClassLoaderTestE/TestE.jar");
    assertNotNull(jarEOrigLocation);

    // Put B into HDFS
    hdfs = TestUtils.getMiniCluster();

    fs = hdfs.getFileSystem();
    assertTrue(fs.mkdirs(new org.apache.hadoop.fs.Path("/contextB")));
    final var dst = new org.apache.hadoop.fs.Path("/contextB/TestB.jar");
    fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(jarBOrigLocation.toURI()), dst);
    assertTrue(fs.exists(dst));
    final URL jarBHdfsLocation = new URL(fs.getUri().toString() + dst.toUri().toString());

    // Have Jetty serve up files from Jar C directory
    var jarCParentDirectory = Path.of(jarCOrigLocation.toURI()).getParent();
    assertNotNull(jarCParentDirectory);
    jetty = TestUtils.getJetty(jarCParentDirectory);
    final URL jarCJettyLocation = jetty.getURI().resolve("TestC.jar").toURL();

    // ContextDefinition with all jars
    var allJarsDef = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation,
        jarBHdfsLocation, jarCJettyLocation, jarDOrigLocation);
    String allJarsDefJson = allJarsDef.toJson();

    // Create local context definition in jar C directory
    File localDefFile = jarCParentDirectory.resolve("allContextDefinition.json").toFile();
    Files.writeString(localDefFile.toPath(), allJarsDefJson, StandardOpenOption.CREATE);
    assertTrue(Files.exists(localDefFile.toPath()));

    var hdfsDefFile = new org.apache.hadoop.fs.Path("/allContextDefinition.json");
    fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(localDefFile.toURI()), hdfsDefFile);
    assertTrue(fs.exists(hdfsDefFile));

    localAllContext = localDefFile.toURI().toURL();
    hdfsAllContext = new URL(fs.getUri().toString() + hdfsDefFile.toUri().toString());
    jettyAllContext = jetty.getURI().resolve("allContextDefinition.json").toURL();

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
  public void beforeEach() throws Exception {
    baseCacheDir = tempDir.resolve("base");
    ConfigurationCopy acuConf = new ConfigurationCopy(
        Map.of(CACHE_DIR_PROPERTY, baseCacheDir.toAbsolutePath().toUri().toURL().toExternalForm()));
    FACTORY = new LocalCachingContextClassLoaderFactory();
    FACTORY.init(() -> new ConfigurationImpl(acuConf));
  }

  @Test
  public void testCreateFromLocal() throws Exception {
    final ClassLoader cl = FACTORY.getClassLoader(localAllContext.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);
  }

  @Test
  public void testCreateFromHdfs() throws Exception {
    final ClassLoader cl = FACTORY.getClassLoader(hdfsAllContext.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);
  }

  @Test
  public void testCreateFromHttp() throws Exception {
    final ClassLoader cl = FACTORY.getClassLoader(jettyAllContext.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);
  }

  @Test
  public void testInvalidContextDefinitionURL() {
    var ex =
        assertThrows(ContextClassLoaderException.class, () -> FACTORY.getClassLoader("/not/a/URL"));
    assertTrue(ex.getCause() instanceof UncheckedIOException);
    assertTrue(ex.getCause().getCause() instanceof MalformedURLException);
    assertEquals("no protocol: /not/a/URL", ex.getCause().getCause().getMessage());
  }

  @Test
  public void testInitialContextDefinitionEmpty() throws Exception {
    // Create a new context definition file in HDFS, but with no content
    final var def = createContextDefinitionFile(fs, "EmptyContextDefinitionFile.json", null);
    final URL emptyDefUrl = new URL(fs.getUri().toString() + def.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(emptyDefUrl.toString()));
    assertTrue(ex.getCause() instanceof UncheckedIOException);
    assertTrue(ex.getCause().getCause() instanceof EOFException);
    assertEquals(
        "InputStream does not contain a valid ContextDefinition at " + emptyDefUrl.toString(),
        ex.getCause().getCause().getMessage());
  }

  @Test
  public void testInitialInvalidJson() throws Exception {
    // Create a new context definition file in HDFS, but with invalid content
    var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    // write out invalid json
    final var invalid = createContextDefinitionFile(fs, "InvalidContextDefinitionFile.json",
        def.toJson().substring(0, 4));
    final URL invalidDefUrl = new URL(fs.getUri().toString() + invalid.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(invalidDefUrl.toString()));
    assertTrue(ex.getCause() instanceof JsonSyntaxException);
    assertTrue(ex.getCause().getCause() instanceof EOFException);
  }

  @Test
  public void testInitial() throws Exception {
    var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var initial =
        createContextDefinitionFile(fs, "InitialContextDefinitionFile.json", def.toJson());
    final URL initialDefUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    ClassLoader cl = FACTORY.getClassLoader(initialDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);
  }

  @Test
  public void testInitialNonExistentResource() throws Exception {
    // copy jarA to some other name
    var jarAPath = Path.of(jarAOrigLocation.toURI());
    var jarAPathParent = jarAPath.getParent();
    assertNotNull(jarAPathParent);
    var jarACopy = jarAPathParent.resolve("jarACopy.jar");
    assertTrue(!Files.exists(jarACopy));
    Files.copy(jarAPath, jarACopy, StandardCopyOption.REPLACE_EXISTING);
    assertTrue(Files.exists(jarACopy));

    var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarACopy.toUri().toURL());

    Files.delete(jarACopy);
    assertTrue(!Files.exists(jarACopy));

    final var initial = createContextDefinitionFile(fs,
        "InitialContextDefinitionFileMissingResource.json", def.toJson());
    final URL initialDefUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(initialDefUrl.toString()));
    boolean foundExpectedException = false;
    var cause = ex.getCause();
    do {
      foundExpectedException =
          cause instanceof FileNotFoundException && cause.getMessage().contains("jarACopy.jar");
      if (cause != null) {
        cause = cause.getCause();
      }
    } while (!foundExpectedException && cause != null);
    assertTrue(foundExpectedException, "Could not find expected FileNotFoundException");
  }

  @Test
  public void testInitialBadResourceURL() throws Exception {
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    resources.add(new Resource(jarAOrigLocation, "fake", "1234"));

    // remove the file:// prefix from the URL
    String goodJson = new ContextDefinition(MONITOR_INTERVAL_SECS, resources).toJson();
    String badJson =
        goodJson.replace(jarAOrigLocation.toString(), jarAOrigLocation.toString().substring(6));
    assertNotEquals(goodJson, badJson);

    final var initial =
        createContextDefinitionFile(fs, "InitialContextDefinitionBadResourceURL.json", badJson);
    final URL initialDefUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(initialDefUrl.toString()));
    assertTrue(ex.getMessage().startsWith("Error getting classloader for context:"),
        ex::getMessage);
    assertTrue(ex.getCause() instanceof JsonSyntaxException);
    assertTrue(ex.getCause().getCause() instanceof MalformedURLException);
    assertTrue(ex.getCause().getCause().getMessage().startsWith("no protocol"),
        ex.getCause().getCause()::getMessage);
  }

  @Test
  public void testInitialBadResourceChecksum() throws Exception {
    Resource r = new Resource(jarAOrigLocation, "fake", "1234");
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    resources.add(r);

    var def = new ContextDefinition(MONITOR_INTERVAL_SECS, resources);

    final var initial = createContextDefinitionFile(fs,
        "InitialContextDefinitionBadResourceChecksum.json", def.toJson());
    final URL initialDefUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(initialDefUrl.toString()));
    assertTrue(ex.getCause() instanceof IllegalStateException);
    assertTrue(ex.getCause().getMessage().startsWith("Error copying resource from file:"),
        ex::getMessage);
    assertTrue(ex.getCause().getCause() instanceof ExecutionException);
    assertTrue(ex.getCause().getCause().getCause() instanceof IllegalStateException);
    assertTrue(ex.getCause().getCause().getCause().getMessage().startsWith("Checksum"),
        ex.getCause().getCause().getCause()::getMessage);
    assertTrue(
        ex.getCause().getCause().getCause().getMessage()
            .endsWith("TestA.jar does not match checksum in context definition 1234"),
        ex.getCause().getCause().getCause()::getMessage);
  }

  @Test
  public void testUpdate() throws Exception {
    final var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var defFilePath =
        createContextDefinitionFile(fs, "UpdateContextDefinitionFile.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // Update the contents of the context definition json file
    var updateDef = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarDOrigLocation);
    updateContextDefinitionFile(fs, defFilePath, updateDef.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(updateDefUrl.toString());

    assertNotEquals(cl, cl2);

    testClassFailsToLoad(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassLoads(cl2, classD);
  }

  @Test
  public void testUpdateSameClassNameDifferentContent() throws Exception {
    final var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var defFilePath =
        createContextDefinitionFile(fs, "UpdateContextDefinitionFile.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // Update the contents of the context definition json file
    var updateDef = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarEOrigLocation);
    updateContextDefinitionFile(fs, defFilePath, updateDef.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(updateDefUrl.toString());

    assertNotEquals(cl, cl2);

    var clazz = cl2.loadClass(classA.getClassName()).asSubclass(test.Test.class);
    test.Test impl = clazz.getDeclaredConstructor().newInstance();
    assertEquals("Hello from E", impl.hello());
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);
  }

  @Test
  public void testUpdateContextDefinitionEmpty() throws Exception {
    final var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var defFilePath =
        createContextDefinitionFile(fs, "UpdateEmptyContextDefinitionFile.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // Update the contents of the context definition json file with an empty file
    updateContextDefinitionFile(fs, defFilePath, null);

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(updateDefUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);

  }

  @Test
  public void testUpdateNonExistentResource() throws Exception {
    final var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var defFilePath =
        createContextDefinitionFile(fs, "UpdateNonExistentResource.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // copy jarA to jarACopy
    // create a ContextDefinition that references it
    // delete jarACopy
    var jarAPath = Path.of(jarAOrigLocation.toURI());
    var jarAPathParent = jarAPath.getParent();
    assertNotNull(jarAPathParent);
    var jarACopy = jarAPathParent.resolve("jarACopy.jar");
    assertTrue(!Files.exists(jarACopy));
    Files.copy(jarAPath, jarACopy, StandardCopyOption.REPLACE_EXISTING);
    assertTrue(Files.exists(jarACopy));
    var def2 = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarACopy.toUri().toURL());
    Files.delete(jarACopy);
    assertTrue(!Files.exists(jarACopy));

    updateContextDefinitionFile(fs, defFilePath, def2.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(updateDefUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);
  }

  @Test
  public void testUpdateBadResourceChecksum() throws Exception {
    final var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var defFilePath =
        createContextDefinitionFile(fs, "UpdateBadResourceChecksum.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    Resource r = new Resource(jarAOrigLocation, "fake", "1234");
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    resources.add(r);

    var def2 = new ContextDefinition(MONITOR_INTERVAL_SECS, resources);

    updateContextDefinitionFile(fs, defFilePath, def2.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(updateDefUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);
  }

  @Test
  public void testUpdateBadResourceURL() throws Exception {
    final var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var defFilePath =
        createContextDefinitionFile(fs, "UpdateBadResourceChecksum.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // remove the file:// prefix from the URL
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    resources.add(new Resource(jarAOrigLocation, "fake", "1234"));
    String goodJson = new ContextDefinition(MONITOR_INTERVAL_SECS, resources).toJson();
    String badJson =
        goodJson.replace(jarAOrigLocation.toString(), jarAOrigLocation.toString().substring(6));
    assertNotEquals(goodJson, badJson);

    updateContextDefinitionFile(fs, defFilePath, badJson);

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(updateDefUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);
  }

  @Test
  public void testUpdateInvalidJson() throws Exception {
    final var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var defFilePath =
        createContextDefinitionFile(fs, "UpdateInvalidContextDefinitionFile.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    var updateDef = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarDOrigLocation);
    updateContextDefinitionFile(fs, defFilePath, updateDef.toJson().substring(0, 4));

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(updateDefUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);

    // Re-write the updated context definition such that it is now valid
    updateContextDefinitionFile(fs, defFilePath, updateDef.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl3 = FACTORY.getClassLoader(updateDefUrl.toString());

    assertEquals(cl, cl2);
    assertNotEquals(cl, cl3);
    testClassFailsToLoad(cl3, classA);
    testClassFailsToLoad(cl3, classB);
    testClassFailsToLoad(cl3, classC);
    testClassLoads(cl3, classD);
  }

  @Test
  public void testChangingContext() throws Exception {
    var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation,
        jarBOrigLocation, jarCOrigLocation, jarDOrigLocation);
    final var update =
        createContextDefinitionFile(fs, "UpdateChangingContextDefinition.json", def.toJson());
    final URL updatedDefUrl = new URL(fs.getUri().toString() + update.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updatedDefUrl.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);

    final List<URL> masterList = new ArrayList<>();
    masterList.add(jarAOrigLocation);
    masterList.add(jarBOrigLocation);
    masterList.add(jarCOrigLocation);
    masterList.add(jarDOrigLocation);

    List<URL> priorList = masterList;
    ClassLoader priorCL = cl;

    for (int i = 0; i < 20; i++) {
      final List<URL> updatedList = new ArrayList<>(masterList);
      Collections.shuffle(updatedList);
      final URL removed = updatedList.remove(0);

      // Update the contents of the context definition json file
      var updateDef = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512",
          updatedList.toArray(new URL[0]));
      updateContextDefinitionFile(fs, update, updateDef.toJson());

      // wait 2x the monitor interval
      Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

      final ClassLoader updatedClassLoader = FACTORY.getClassLoader(updatedDefUrl.toString());

      if (updatedList.equals(priorList)) {
        assertEquals(priorCL, updatedClassLoader);
      } else {
        assertNotEquals(cl, updatedClassLoader);
        for (URL u : updatedList) {
          if (u.toString().equals(jarAOrigLocation.toString())) {
            testClassLoads(updatedClassLoader, classA);
          } else if (u.toString().equals(jarBOrigLocation.toString())) {
            testClassLoads(updatedClassLoader, classB);
          } else if (u.toString().equals(jarCOrigLocation.toString())) {
            testClassLoads(updatedClassLoader, classC);
          } else if (u.toString().equals(jarDOrigLocation.toString())) {
            testClassLoads(updatedClassLoader, classD);
          } else {
            fail("Unexpected url: " + u.toString());
          }
        }
      }
      if (removed.toString().equals(jarAOrigLocation.toString())) {
        testClassFailsToLoad(updatedClassLoader, classA);
      } else if (removed.toString().equals(jarBOrigLocation.toString())) {
        testClassFailsToLoad(updatedClassLoader, classB);
      } else if (removed.toString().equals(jarCOrigLocation.toString())) {
        testClassFailsToLoad(updatedClassLoader, classC);
      } else if (removed.toString().equals(jarDOrigLocation.toString())) {
        testClassFailsToLoad(updatedClassLoader, classD);
      } else {
        fail("Unexpected url: " + removed.toString());
      }
      priorCL = updatedClassLoader;
      priorList = updatedList;
    }
  }

  @Test
  public void testGracePeriod() throws Exception {
    final LocalCachingContextClassLoaderFactory localFactory =
        new LocalCachingContextClassLoaderFactory();

    String baseCacheDir = tempDir.resolve("base").toUri().toString();
    ConfigurationCopy acuConf = new ConfigurationCopy(
        Map.of(CACHE_DIR_PROPERTY, baseCacheDir, UPDATE_FAILURE_GRACE_PERIOD_MINS, "1"));
    localFactory.init(() -> new ConfigurationImpl(acuConf));

    final var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var defFilePath =
        createContextDefinitionFile(fs, "UpdateNonExistentResource.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = localFactory.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // copy jarA to jarACopy
    // create a ContextDefinition that references it
    // delete jarACopy
    var jarAPath = Path.of(jarAOrigLocation.toURI());
    var jarAPathParent = jarAPath.getParent();
    assertNotNull(jarAPathParent);
    var jarACopy = jarAPathParent.resolve("jarACopy.jar");
    assertTrue(!Files.exists(jarACopy));
    Files.copy(jarAPath, jarACopy, StandardCopyOption.REPLACE_EXISTING);
    assertTrue(Files.exists(jarACopy));
    var def2 = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarACopy.toUri().toURL());
    Files.delete(jarACopy);
    assertTrue(!Files.exists(jarACopy));

    updateContextDefinitionFile(fs, defFilePath, def2.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = localFactory.getClassLoader(updateDefUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);

    // Wait 2 minutes for grace period to expire
    Thread.sleep(120_000);

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> localFactory.getClassLoader(updateDefUrl.toString()));
    boolean foundExpectedException = false;
    var cause = ex.getCause();
    do {
      foundExpectedException =
          cause instanceof FileNotFoundException && cause.getMessage().contains("jarACopy.jar");
      if (cause != null) {
        cause = cause.getCause();
      }
    } while (!foundExpectedException && cause != null);
    assertTrue(foundExpectedException, "Could not find expected FileNotFoundException");
  }

  @Test
  public void testExternalFileModification() throws Exception {
    var def = ContextDefinition.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation,
        jarBOrigLocation, jarCOrigLocation, jarDOrigLocation);
    final var update =
        createContextDefinitionFile(fs, "UpdateChangingContextDefinition.json", def.toJson());
    final URL updatedDefUrl = new URL(fs.getUri().toString() + update.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updatedDefUrl.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);

    var resources = tempDir.resolve("base").resolve("resources");
    List<Path> files =
        def.getResources().stream().map(r -> resources.resolve(LocalStore.localResourceName(r)))
            .limit(2).collect(Collectors.toList());
    assertEquals(2, files.size());

    // overwrite one downloaded jar with others content
    Files.copy(files.get(0), files.get(1), StandardCopyOption.REPLACE_EXISTING);

    final var update2 =
        createContextDefinitionFile(fs, "UpdateChangingContextDefinition2.json", def.toJson());
    final URL updatedDefUrl2 = new URL(fs.getUri().toString() + update2.toUri().toString());

    // The classloader should fail to create because one of the files in the local filesystem cache
    // has a checksum mismatch
    var exception = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(updatedDefUrl2.toString()));
    assertTrue(exception.getMessage().contains("Checksum"), exception::getMessage);

    // clean up corrupt file
    Files.delete(files.get(1));

    // ensure it works now
    FACTORY.getClassLoader(updatedDefUrl2.toString());
  }
}
