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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;
import org.apache.accumulo.classloader.lcc.definition.Resource;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LocalCachingContextClassLoaderFactoryTest {

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

    // Put C into Jetty
    java.nio.file.Path jarCParentDirectory = Paths.get(jarCOrigLocation.toURI()).getParent();
    assertNotNull(jarCParentDirectory);
    jetty = TestUtils.getJetty(jarCParentDirectory);
    final URL jarCJettyLocation = jetty.getURI().resolve("TestC.jar").toURL();

    // ContextDefinition with all jars
    ContextDefinition allJarsDef = createContextDef("all", jarAOrigLocation, jarBHdfsLocation,
        jarCJettyLocation, jarDOrigLocation);
    String allJarsDefJson = allJarsDef.toJson();
    System.out.println(allJarsDefJson);

    File localDefFile = new File(jarCParentDirectory.toFile(), "allContextDefinition.json");
    Files.writeString(localDefFile.toPath(), allJarsDefJson, StandardOpenOption.CREATE);
    assertTrue(Files.exists(localDefFile.toPath()));

    Path hdfsDefFile = new Path("/allContextDefinition.json");
    fs.copyFromLocalFile(new Path(localDefFile.toURI()), hdfsDefFile);
    assertTrue(fs.exists(hdfsDefFile));

    localAllContext = localDefFile.toURI().toURL();
    hdfsAllContext = new URL(fs.getUri().toString() + hdfsDefFile.toUri().toString());
    jettyAllContext = jetty.getURI().resolve("allContextDefinition.json").toURL();

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

  @Test
  public void testInvalidContextDefinitionURL() {
    LocalCachingContextClassLoaderFactory factory = new LocalCachingContextClassLoaderFactory();
    ContextClassLoaderException ex =
        assertThrows(ContextClassLoaderException.class, () -> factory.getClassLoader("/not/a/URL"));
    assertEquals("Error getting classloader for context: Expected valid URL to context definition "
        + "file but received: /not/a/URL", ex.getMessage());
  }

  @Test
  public void testInitialContextDefinitionEmpty() throws Exception {
    // Create a new context definition file in HDFS, but with no content
    assertTrue(fs.mkdirs(new Path("/contextDefs")));
    final Path empty = new Path("/contextDefs/EmptyContextDefinitionFile.json");
    assertTrue(fs.createNewFile(empty));
    assertTrue(fs.exists(empty));
    final URL emptyDefUrl = new URL(fs.getUri().toString() + empty.toUri().toString());

    LocalCachingContextClassLoaderFactory factory = new LocalCachingContextClassLoaderFactory();
    ContextClassLoaderException ex = assertThrows(ContextClassLoaderException.class,
        () -> factory.getClassLoader(emptyDefUrl.toString()));
    assertEquals(
        "Error getting classloader for context: ContextDefinition null for context definition "
            + "file: " + emptyDefUrl.toString(),
        ex.getMessage());
  }

  @Test
  public void testInitialContextDefinitionInvalid() throws Exception {
    // Create a new context definition file in HDFS, but with invalid content
    assertTrue(fs.mkdirs(new Path("/contextDefs")));
    final Path invalid = new Path("/contextDefs/InvalidContextDefinitionFile.json");
    try (FSDataOutputStream out = fs.create(invalid)) {
      ContextDefinition def = createContextDef("invalid", jarAOrigLocation);
      out.writeBytes(def.toJson().substring(0, 4));
    }
    assertTrue(fs.exists(invalid));
    final URL invalidDefUrl = new URL(fs.getUri().toString() + invalid.toUri().toString());

    LocalCachingContextClassLoaderFactory factory = new LocalCachingContextClassLoaderFactory();
    ContextClassLoaderException ex = assertThrows(ContextClassLoaderException.class,
        () -> factory.getClassLoader(invalidDefUrl.toString()));
    assertTrue(ex.getMessage().startsWith(
        "Error getting classloader for context: com.google.gson.stream.MalformedJsonException"));
  }

  @Test
  public void testInitial() throws Exception {
    // Create a new context definition file in HDFS, but with invalid content
    assertTrue(fs.mkdirs(new Path("/contextDefs")));
    final Path initial = new Path("/contextDefs/InitialContextDefinitionFile.json");
    try (FSDataOutputStream out = fs.create(initial)) {
      ContextDefinition def = createContextDef("initial", jarAOrigLocation);
      out.writeBytes(def.toJson());
    }
    assertTrue(fs.exists(initial));
    final URL initialDefUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    LocalCachingContextClassLoaderFactory factory = new LocalCachingContextClassLoaderFactory();
    ClassLoader cl = factory.getClassLoader(initialDefUrl.toString());
    @SuppressWarnings("unchecked")
    Class<? extends test.Test> clazzA =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectA");
    test.Test a1 = clazzA.getDeclaredConstructor().newInstance();
    assertEquals("Hello from A", a1.hello());
  }

  @Test
  public void testUpdate() throws Exception {
    // Create a new context definition file in HDFS
    assertTrue(fs.mkdirs(new Path("/contextDefs")));
    final Path defFilePath = new Path("/contextDefs/UpdateContextDefinitionFile.json");
    final ContextDefinition def = createContextDef("update", jarAOrigLocation);
    try (FSDataOutputStream out = fs.create(defFilePath)) {
      out.writeBytes(def.toJson());
    }
    assertTrue(fs.exists(defFilePath));
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final LocalCachingContextClassLoaderFactory factory =
        new LocalCachingContextClassLoaderFactory();
    final ClassLoader cl = factory.getClassLoader(updateDefUrl.toString());
    @SuppressWarnings("unchecked")
    Class<? extends test.Test> clazzA =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectA");
    test.Test a1 = clazzA.getDeclaredConstructor().newInstance();
    assertEquals("Hello from A", a1.hello());

    // Update the contents of the context definition json file
    fs.delete(defFilePath, false);
    assertFalse(fs.exists(defFilePath));

    ContextDefinition updateDef = createContextDef("update", jarDOrigLocation);
    try (FSDataOutputStream out = fs.create(defFilePath)) {
      out.writeBytes(updateDef.toJson());
    }
    assertTrue(fs.exists(defFilePath));

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = factory.getClassLoader(updateDefUrl.toString());
    assertThrows(ClassNotFoundException.class, () -> cl2.loadClass("test.TestObjectA"));

    @SuppressWarnings("unchecked")
    Class<? extends test.Test> clazzD =
        (Class<? extends test.Test>) cl2.loadClass("test.TestObjectD");
    test.Test d1 = clazzD.getDeclaredConstructor().newInstance();
    assertEquals("Hello from D", d1.hello());

  }

  @Test
  public void testUpdateInvalid() throws Exception {
    // Create a new context definition file in HDFS, but with invalid content
    assertTrue(fs.mkdirs(new Path("/contextDefs")));
    final Path defFilePath = new Path("/contextDefs/UpdateContextDefinitionFile.json");
    final ContextDefinition def = createContextDef("update", jarAOrigLocation);
    try (FSDataOutputStream out = fs.create(defFilePath)) {
      out.writeBytes(def.toJson());
    }
    assertTrue(fs.exists(defFilePath));
    final URL updateDefUrl = new URL(fs.getUri().toString() + defFilePath.toUri().toString());

    final LocalCachingContextClassLoaderFactory factory =
        new LocalCachingContextClassLoaderFactory();
    final ClassLoader cl = factory.getClassLoader(updateDefUrl.toString());
    @SuppressWarnings("unchecked")
    Class<? extends test.Test> clazzA =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectA");
    test.Test a1 = clazzA.getDeclaredConstructor().newInstance();
    assertEquals("Hello from A", a1.hello());

    // Update the contents of the context definition json file
    fs.delete(defFilePath, false);
    assertFalse(fs.exists(defFilePath));

    ContextDefinition updateDef = createContextDef("update", jarDOrigLocation);
    try (FSDataOutputStream out = fs.create(defFilePath)) {
      out.writeBytes(updateDef.toJson().substring(0, 4));
    }
    assertTrue(fs.exists(defFilePath));

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = factory.getClassLoader(updateDefUrl.toString());

    @SuppressWarnings("unchecked")
    Class<? extends test.Test> clazzA2 =
        (Class<? extends test.Test>) cl2.loadClass("test.TestObjectA");
    test.Test a2 = clazzA2.getDeclaredConstructor().newInstance();
    assertEquals("Hello from A", a2.hello());
    assertEquals(clazzA, clazzA2);

    assertThrows(ClassNotFoundException.class, () -> cl2.loadClass("test.TestObjectD"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCreateFromLocal() throws Exception {
    LocalCachingContextClassLoaderFactory factory = new LocalCachingContextClassLoaderFactory();
    final ClassLoader cl = factory.getClassLoader(localAllContext.toString());

    final Class<? extends test.Test> clazzA =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectA");
    test.Test a1 = clazzA.getDeclaredConstructor().newInstance();
    assertEquals("Hello from A", a1.hello());

    final Class<? extends test.Test> clazzB =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectB");
    test.Test b1 = clazzB.getDeclaredConstructor().newInstance();
    assertEquals("Hello from B", b1.hello());

    final Class<? extends test.Test> clazzC =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectC");
    test.Test c1 = clazzC.getDeclaredConstructor().newInstance();
    assertEquals("Hello from C", c1.hello());

    final Class<? extends test.Test> clazzD =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectD");
    test.Test d1 = clazzD.getDeclaredConstructor().newInstance();
    assertEquals("Hello from D", d1.hello());

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCreateFromHdfs() throws Exception {
    LocalCachingContextClassLoaderFactory factory = new LocalCachingContextClassLoaderFactory();
    final ClassLoader cl = factory.getClassLoader(hdfsAllContext.toString());

    final Class<? extends test.Test> clazzA =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectA");
    test.Test a1 = clazzA.getDeclaredConstructor().newInstance();
    assertEquals("Hello from A", a1.hello());

    final Class<? extends test.Test> clazzB =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectB");
    test.Test b1 = clazzB.getDeclaredConstructor().newInstance();
    assertEquals("Hello from B", b1.hello());

    final Class<? extends test.Test> clazzC =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectC");
    test.Test c1 = clazzC.getDeclaredConstructor().newInstance();
    assertEquals("Hello from C", c1.hello());

    final Class<? extends test.Test> clazzD =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectD");
    test.Test d1 = clazzD.getDeclaredConstructor().newInstance();
    assertEquals("Hello from D", d1.hello());

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCreateFromHttp() throws Exception {
    LocalCachingContextClassLoaderFactory factory = new LocalCachingContextClassLoaderFactory();
    final ClassLoader cl = factory.getClassLoader(jettyAllContext.toString());

    final Class<? extends test.Test> clazzA =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectA");
    test.Test a1 = clazzA.getDeclaredConstructor().newInstance();
    assertEquals("Hello from A", a1.hello());

    final Class<? extends test.Test> clazzB =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectB");
    test.Test b1 = clazzB.getDeclaredConstructor().newInstance();
    assertEquals("Hello from B", b1.hello());

    final Class<? extends test.Test> clazzC =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectC");
    test.Test c1 = clazzC.getDeclaredConstructor().newInstance();
    assertEquals("Hello from C", c1.hello());

    final Class<? extends test.Test> clazzD =
        (Class<? extends test.Test>) cl.loadClass("test.TestObjectD");
    test.Test d1 = clazzD.getDeclaredConstructor().newInstance();
    assertEquals("Hello from D", d1.hello());

  }

  private static ContextDefinition createContextDef(String contextName, URL... sources)
      throws ContextClassLoaderException, IOException {
    List<Resource> resources = new ArrayList<>();
    for (URL u : sources) {
      FileResolver resolver = FileResolver.resolve(u);
      try (InputStream is = resolver.getInputStream()) {
        String checksum = Constants.getChecksummer().digestAsHex(is);
        resources.add(new Resource(u.toString(), checksum));
      }
    }
    return new ContextDefinition(contextName, MONITOR_INTERVAL_SECS, resources);
  }

}
