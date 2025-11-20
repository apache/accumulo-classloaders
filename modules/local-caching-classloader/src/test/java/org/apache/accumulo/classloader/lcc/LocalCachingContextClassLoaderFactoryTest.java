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

import static org.apache.accumulo.classloader.lcc.TestUtils.createContextDefinitionFile;
import static org.apache.accumulo.classloader.lcc.TestUtils.testClassFailsToLoad;
import static org.apache.accumulo.classloader.lcc.TestUtils.testClassLoads;
import static org.apache.accumulo.classloader.lcc.TestUtils.updateContextDefinitionFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.TreeSet;

import org.apache.accumulo.classloader.lcc.TestUtils.TestClassInfo;
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

public class LocalCachingContextClassLoaderFactoryTest {

  private static final LocalCachingContextClassLoaderFactory FACTORY =
      new LocalCachingContextClassLoaderFactory();
  private static final int MONITOR_INTERVAL_SECS = 5;
  private static MiniDFSCluster hdfs;
  private static FileSystem fs;
  private static Server jetty;
  private static URL jarAOrigLocation;
  private static URL jarBOrigLocation;
  private static URL jarCOrigLocation;
  private static URL jarDOrigLocation;
  private static URL localAllContext;
  private static URL hdfsAllContext;
  private static URL jettyAllContext;
  private static TestClassInfo classA;
  private static TestClassInfo classB;
  private static TestClassInfo classC;
  private static TestClassInfo classD;

  @TempDir
  private static java.nio.file.Path tempDir;

  @BeforeAll
  public static void beforeAll() throws Exception {
    String tmp = tempDir.resolve("base").toUri().toString();
    System.setProperty(Constants.CACHE_DIR_PROPERTY, tmp);

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

    // Put B into HDFS
    hdfs = TestUtils.getMiniCluster();
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(hdfs.getConfiguration(0)));

    fs = hdfs.getFileSystem();
    assertTrue(fs.mkdirs(new Path("/contextB")));
    final Path dst = new Path("/contextB/TestB.jar");
    fs.copyFromLocalFile(new Path(jarBOrigLocation.toURI()), dst);
    assertTrue(fs.exists(dst));
    final URL jarBHdfsLocation = new URL(fs.getUri().toString() + dst.toUri().toString());

    // Have Jetty serve up files from Jar C directory
    java.nio.file.Path jarCParentDirectory =
        java.nio.file.Path.of(jarCOrigLocation.toURI()).getParent();
    assertNotNull(jarCParentDirectory);
    jetty = TestUtils.getJetty(jarCParentDirectory);
    final URL jarCJettyLocation = jetty.getURI().resolve("TestC.jar").toURL();

    // ContextDefinition with all jars
    ContextDefinition allJarsDef = ContextDefinition.create("all", MONITOR_INTERVAL_SECS,
        jarAOrigLocation, jarBHdfsLocation, jarCJettyLocation, jarDOrigLocation);
    String allJarsDefJson = allJarsDef.toJson();

    // Create local context definition in jar C directory
    File localDefFile = jarCParentDirectory.resolve("allContextDefinition.json").toFile();
    Files.writeString(localDefFile.toPath(), allJarsDefJson, StandardOpenOption.CREATE);
    assertTrue(Files.exists(localDefFile.toPath()));

    Path hdfsDefFile = new Path("/allContextDefinition.json");
    fs.copyFromLocalFile(new Path(localDefFile.toURI()), hdfsDefFile);
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
    System.clearProperty(Constants.CACHE_DIR_PROPERTY);
    if (jetty != null) {
      jetty.stop();
      jetty.join();
    }
    if (hdfs != null) {
      hdfs.shutdown();
    }
  }

  @AfterEach
  public void afterEach() {
    FACTORY.resetForTests();
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
    ContextClassLoaderException ex =
        assertThrows(ContextClassLoaderException.class, () -> FACTORY.getClassLoader("/not/a/URL"));
    assertEquals("Error getting classloader for context: Expected valid URL to context definition "
        + "file but received: /not/a/URL", ex.getMessage());
  }

  @Test
  public void testInitialContextDefinitionEmpty() throws Exception {
    // Create a new context definition file in HDFS, but with no content
    final Path def = createContextDefinitionFile(fs, "EmptyContextDefinitionFile.json", null);
    final URL emptyDefUrl = new URL(fs.getUri().toString() + def.toUri().toString());

    ContextClassLoaderException ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(emptyDefUrl.toString()));
    assertEquals(
        "Error getting classloader for context: ContextDefinition null for context definition "
            + "file: " + emptyDefUrl.toString(),
        ex.getMessage());
  }

  @Test
  public void testInitialInvalidJson() throws Exception {
    // Create a new context definition file in HDFS, but with invalid content
    ContextDefinition def =
        ContextDefinition.create("invalid", MONITOR_INTERVAL_SECS, jarAOrigLocation);
    // write out invalid json
    final Path invalid = createContextDefinitionFile(fs, "InvalidContextDefinitionFile.json",
        def.toJson().substring(0, 4));
    final URL invalidDefUrl = new URL(fs.getUri().toString() + invalid.toUri().toString());

    ContextClassLoaderException ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(invalidDefUrl.toString()));
    assertTrue(ex.getMessage().startsWith(
        "Error getting classloader for context: com.google.gson.stream.MalformedJsonException"));
  }

  @Test
  public void testInitial() throws Exception {
    ContextDefinition def =
        ContextDefinition.create("initial", MONITOR_INTERVAL_SECS, jarAOrigLocation);
    final Path initial =
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
    java.nio.file.Path jarAPath = java.nio.file.Path.of(jarAOrigLocation.toURI());
    java.nio.file.Path jarAPathParent = jarAPath.getParent();
    assertNotNull(jarAPathParent);
    java.nio.file.Path jarACopy = jarAPathParent.resolve("jarACopy.jar");
    assertTrue(!Files.exists(jarACopy));
    Files.copy(jarAPath, jarACopy, StandardCopyOption.REPLACE_EXISTING);
    assertTrue(Files.exists(jarACopy));

    ContextDefinition def =
        ContextDefinition.create("initial", MONITOR_INTERVAL_SECS, jarACopy.toUri().toURL());

    Files.delete(jarACopy);
    assertTrue(!Files.exists(jarACopy));

    final Path initial = createContextDefinitionFile(fs,
        "InitialContextDefinitionFileMissingResource.json", def.toJson());
    final URL initialDefUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    ContextClassLoaderException ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(initialDefUrl.toString()));
    assertTrue(ex.getMessage().endsWith("jarACopy.jar does not exist."));
  }

  @Test
  public void testInitialBadResourceURL() throws Exception {
    Resource r = new Resource();
    // remove the file:// prefix from the URL
    r.setLocation(jarAOrigLocation.toString().substring(6));
    r.setChecksum("1234");
    TreeSet<Resource> resources = new TreeSet<>();
    resources.add(r);

    ContextDefinition def = new ContextDefinition();
    def.setContextName("initial");
    def.setMonitorIntervalSeconds(MONITOR_INTERVAL_SECS);
    def.setResources(resources);

    final Path initial = createContextDefinitionFile(fs,
        "InitialContextDefinitionBadResourceURL.json", def.toJson());
    final URL initialDefUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    ContextClassLoaderException ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(initialDefUrl.toString()));
    assertTrue(ex.getMessage().startsWith("Error getting classloader for context: no protocol"));
    Throwable t = ex.getCause();
    assertTrue(t instanceof MalformedURLException);
    assertTrue(t.getMessage().startsWith("no protocol"));
  }

  @Test
  public void testInitialBadResourceChecksum() throws Exception {
    Resource r = new Resource();
    r.setLocation(jarAOrigLocation.toString());
    r.setChecksum("1234");
    TreeSet<Resource> resources = new TreeSet<>();
    resources.add(r);

    ContextDefinition def = new ContextDefinition();
    def.setContextName("initial");
    def.setMonitorIntervalSeconds(MONITOR_INTERVAL_SECS);
    def.setResources(resources);

    final Path initial = createContextDefinitionFile(fs,
        "InitialContextDefinitionBadResourceChecksum.json", def.toJson());
    final URL initialDefUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    ContextClassLoaderException ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(initialDefUrl.toString()));
    assertTrue(ex.getMessage().startsWith("Error getting classloader for context: Checksum"));
    Throwable t = ex.getCause();
    assertTrue(t instanceof IllegalStateException);
    assertTrue(
        t.getMessage().endsWith("TestA.jar does not match checksum in context definition 1234"));
  }

  @Test
  public void testUpdate() throws Exception {
    final ContextDefinition def =
        ContextDefinition.create("update", MONITOR_INTERVAL_SECS, jarAOrigLocation);
    final Path defFilePath =
        createContextDefinitionFile(fs, "UpdateContextDefinitionFile.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // Update the contents of the context definition json file
    ContextDefinition updateDef =
        ContextDefinition.create("update", MONITOR_INTERVAL_SECS, jarDOrigLocation);
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
  public void testUpdateContextDefinitionEmpty() throws Exception {
    final ContextDefinition def =
        ContextDefinition.create("update", MONITOR_INTERVAL_SECS, jarAOrigLocation);
    final Path defFilePath =
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
    final ContextDefinition def =
        ContextDefinition.create("update", MONITOR_INTERVAL_SECS, jarAOrigLocation);
    final Path defFilePath =
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
    java.nio.file.Path jarAPath = java.nio.file.Path.of(jarAOrigLocation.toURI());
    java.nio.file.Path jarAPathParent = jarAPath.getParent();
    assertNotNull(jarAPathParent);
    java.nio.file.Path jarACopy = jarAPathParent.resolve("jarACopy.jar");
    assertTrue(!Files.exists(jarACopy));
    Files.copy(jarAPath, jarACopy, StandardCopyOption.REPLACE_EXISTING);
    assertTrue(Files.exists(jarACopy));
    ContextDefinition def2 =
        ContextDefinition.create("initial", MONITOR_INTERVAL_SECS, jarACopy.toUri().toURL());
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
    final ContextDefinition def =
        ContextDefinition.create("update", MONITOR_INTERVAL_SECS, jarAOrigLocation);
    final Path defFilePath =
        createContextDefinitionFile(fs, "UpdateBadResourceChecksum.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    Resource r = new Resource();
    r.setLocation(jarAOrigLocation.toString());
    r.setChecksum("1234");
    TreeSet<Resource> resources = new TreeSet<>();
    resources.add(r);

    ContextDefinition def2 = new ContextDefinition();
    def2.setContextName("update");
    def2.setMonitorIntervalSeconds(MONITOR_INTERVAL_SECS);
    def2.setResources(resources);

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
    final ContextDefinition def =
        ContextDefinition.create("update", MONITOR_INTERVAL_SECS, jarAOrigLocation);
    final Path defFilePath =
        createContextDefinitionFile(fs, "UpdateBadResourceChecksum.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    Resource r = new Resource();
    // remove the file:// prefix from the URL
    r.setLocation(jarAOrigLocation.toString().substring(6));
    r.setChecksum("1234");
    TreeSet<Resource> resources = new TreeSet<>();
    resources.add(r);

    ContextDefinition def2 = new ContextDefinition();
    def2.setContextName("initial");
    def2.setMonitorIntervalSeconds(MONITOR_INTERVAL_SECS);
    def2.setResources(resources);

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
  public void testUpdateInvalidJson() throws Exception {
    final ContextDefinition def =
        ContextDefinition.create("update", MONITOR_INTERVAL_SECS, jarAOrigLocation);
    final Path defFilePath =
        createContextDefinitionFile(fs, "UpdateInvalidContextDefinitionFile.json", def.toJson());
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateDefUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    ContextDefinition updateDef =
        ContextDefinition.create("update", MONITOR_INTERVAL_SECS, jarDOrigLocation);
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

}
