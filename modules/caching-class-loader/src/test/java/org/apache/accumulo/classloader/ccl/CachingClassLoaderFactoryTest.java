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
package org.apache.accumulo.classloader.ccl;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.accumulo.classloader.ccl.CachingClassLoaderFactory.PROP_ALLOWED_URLS;
import static org.apache.accumulo.classloader.ccl.CachingClassLoaderFactory.PROP_CACHE_DIR;
import static org.apache.accumulo.classloader.ccl.CachingClassLoaderFactory.PROP_GRACE_PERIOD;
import static org.apache.accumulo.classloader.ccl.LocalStore.RESOURCES_DIR;
import static org.apache.accumulo.classloader.ccl.LocalStore.WORKING_DIR;
import static org.apache.accumulo.classloader.ccl.TestUtils.createContextManifestFile;
import static org.apache.accumulo.classloader.ccl.TestUtils.testClassFailsToLoad;
import static org.apache.accumulo.classloader.ccl.TestUtils.testClassLoads;
import static org.apache.accumulo.classloader.ccl.TestUtils.updateContextManifestFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.accumulo.classloader.ccl.TestUtils.TestClassInfo;
import org.apache.accumulo.classloader.ccl.manifest.ContextManifest;
import org.apache.accumulo.classloader.ccl.manifest.Resource;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.accumulo.core.util.Timer;
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

import com.google.gson.JsonSyntaxException;

class CachingClassLoaderFactoryTest {

  protected static final int MONITOR_INTERVAL_SECS = 5;
  // MD5 sum for "bad"
  private static final String BAD_MD5 = "bae60998ffe4923b131e3d6e4c19993e";
  private static MiniDFSCluster hdfs;
  private static FileSystem fs;
  private static Server jetty;
  private static URL jarAOrigLocation;
  private static URL jarBOrigLocation;
  private static URL jarCOrigLocation;
  private static URL jarDOrigLocation;
  private static URL jarEOrigLocation;
  private static URL localAllUrl;
  private static URL hdfsAllUrl;
  private static URL jettyAllUrl;
  private static TestClassInfo classA;
  private static TestClassInfo classB;
  private static TestClassInfo classC;
  private static TestClassInfo classD;

  private CachingClassLoaderFactory FACTORY;
  private Path baseCacheDir;

  @TempDir(cleanup = CleanupMode.ON_SUCCESS)
  private Path tempDir;

  @BeforeAll
  public static void beforeAll() throws Exception {
    // Find the Test jar files
    jarAOrigLocation =
        CachingClassLoaderFactoryTest.class.getResource("/ClassLoaderTestA/TestA.jar");
    assertNotNull(jarAOrigLocation);
    jarBOrigLocation =
        CachingClassLoaderFactoryTest.class.getResource("/ClassLoaderTestB/TestB.jar");
    assertNotNull(jarBOrigLocation);
    jarCOrigLocation =
        CachingClassLoaderFactoryTest.class.getResource("/ClassLoaderTestC/TestC.jar");
    assertNotNull(jarCOrigLocation);
    jarDOrigLocation =
        CachingClassLoaderFactoryTest.class.getResource("/ClassLoaderTestD/TestD.jar");
    assertNotNull(jarDOrigLocation);
    jarEOrigLocation =
        CachingClassLoaderFactoryTest.class.getResource("/ClassLoaderTestE/TestE.jar");
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

    // manifest with all jars
    var allJars = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation,
        jarBHdfsLocation, jarCJettyLocation, jarDOrigLocation);
    String allJarsJson = allJars.toJson();

    // Create local manifest in jar C directory
    File localFile = jarCParentDirectory.resolve("all.json").toFile();
    Files.writeString(localFile.toPath(), allJarsJson);
    assertTrue(Files.exists(localFile.toPath()));

    var hdfsFile = new org.apache.hadoop.fs.Path("/all.json");
    fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(localFile.toURI()), hdfsFile);
    assertTrue(fs.exists(hdfsFile));

    localAllUrl = localFile.toURI().toURL();
    hdfsAllUrl = new URL(fs.getUri().toString() + hdfsFile.toUri().toString());
    jettyAllUrl = jetty.getURI().resolve("all.json").toURL();

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
  public void beforeEach(TestInfo info) throws Exception {
    baseCacheDir = tempDir.resolve(info.getTestMethod().orElseThrow().getName());
    ConfigurationCopy acuConf = new ConfigurationCopy(
        Map.of(PROP_CACHE_DIR, baseCacheDir.toAbsolutePath().toUri().toURL().toExternalForm(),
            PROP_GRACE_PERIOD, "1", PROP_ALLOWED_URLS, ".*"));
    FACTORY = new CachingClassLoaderFactory();
    FACTORY.init(() -> new ConfigurationImpl(acuConf));
  }

  @Test
  public void testAllowedUrls() throws Exception {
    // use a different factory than other tests; only allow file: URLs
    ConfigurationCopy acuConf = new ConfigurationCopy(
        Map.of(PROP_CACHE_DIR, baseCacheDir.toAbsolutePath().toUri().toURL().toExternalForm(),
            PROP_ALLOWED_URLS, "file:.*"));
    var factory = new CachingClassLoaderFactory();
    factory.init(() -> new ConfigurationImpl(acuConf));

    // case 1: all URLs pass (normal case, covered by other tests)

    // case 2: manifest URL fails to match the pattern
    var ex = assertThrows(ContextClassLoaderException.class,
        () -> factory.getClassLoader(hdfsAllUrl.toExternalForm()));
    assertTrue(ex.getCause() instanceof IllegalArgumentException);
    assertTrue(ex.getCause().getMessage().contains("Context manifest URL (hdfs:"));

    // case 3a: manifest URL matches, but resource URL should fail to match the pattern,
    // but it works anyway, because the resources were downloaded already by a different instance
    // (in this case, by a less restrictive FACTORY instance) and no new connection is made
    FACTORY.getClassLoader(hdfsAllUrl.toExternalForm());
    factory.getClassLoader(localAllUrl.toExternalForm()); // same resources

    // case 3b: manifest URL matches, but resource URL fails to match the pattern
    // in this case, we use a new manifest, with a resource that doesn't exist locally
    var newResources = new LinkedHashSet<Resource>();
    var badUrl = "http://localhost/some/path";
    newResources.add(new Resource(new URL(badUrl), "MD5", BAD_MD5));
    var context2 = new ContextManifest(MONITOR_INTERVAL_SECS, newResources);
    var disallowedContext = tempDir.resolve("context-with-disallowed-resource-url.json");
    Files.writeString(disallowedContext, context2.toJson());
    ex = assertThrows(ContextClassLoaderException.class,
        () -> factory.getClassLoader(disallowedContext.toUri().toURL().toExternalForm()));
    assertTrue(ex.getCause() instanceof IllegalStateException);
    assertTrue(ex.getCause().getCause() instanceof ExecutionException);
    assertTrue(ex.getCause().getCause().getCause() instanceof IllegalArgumentException);
    assertTrue(
        ex.getCause().getCause().getCause().getMessage()
            .contains("Context resource URL (" + badUrl + ") not allowed by pattern ("),
        ex.getCause().getCause().getCause()::getMessage);

    // case 4: invalid regex for allowed url
    ConfigurationCopy acuConf2 = new ConfigurationCopy(
        Map.of(PROP_CACHE_DIR, baseCacheDir.toAbsolutePath().toUri().toURL().toExternalForm(),
            PROP_ALLOWED_URLS, "file:[a-z.*"));
    var factory2 = new CachingClassLoaderFactory();
    factory2.init(() -> new ConfigurationImpl(acuConf2));
    ex = assertThrows(ContextClassLoaderException.class,
        () -> factory2.getClassLoader(hdfsAllUrl.toExternalForm()));
    assertEquals(PatternSyntaxException.class, ex.getCause().getClass());

    // case 5: no allowed pattern url is set
    ConfigurationCopy acuConf3 = new ConfigurationCopy(
        Map.of(PROP_CACHE_DIR, baseCacheDir.toAbsolutePath().toUri().toURL().toExternalForm()));
    var factory3 = new CachingClassLoaderFactory();
    factory3.init(() -> new ConfigurationImpl(acuConf3));
    ex = assertThrows(ContextClassLoaderException.class,
        () -> factory3.getClassLoader(hdfsAllUrl.toExternalForm()));
    assertTrue(ex.getMessage().contains(PROP_ALLOWED_URLS + " not set"), ex::getMessage);
  }

  @Test
  public void testCreateFromLocal() throws Exception {
    final ClassLoader cl = FACTORY.getClassLoader(localAllUrl.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);
  }

  @Test
  public void testCreateFromHdfs() throws Exception {
    final ClassLoader cl = FACTORY.getClassLoader(hdfsAllUrl.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);
  }

  @Test
  public void testCreateFromHttp() throws Exception {
    final ClassLoader cl = FACTORY.getClassLoader(jettyAllUrl.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);
  }

  @Test
  public void testInvalidManifestURL() {
    var ex =
        assertThrows(ContextClassLoaderException.class, () -> FACTORY.getClassLoader("/not/a/URL"));
    assertTrue(ex.getCause() instanceof UncheckedIOException);
    assertTrue(ex.getCause().getCause() instanceof MalformedURLException);
    assertEquals("no protocol: /not/a/URL", ex.getCause().getCause().getMessage());
  }

  @Test
  public void testInitialManifestEmpty() throws Exception {
    // Create a new manifest file in HDFS, but with no content
    final var manifest = createContextManifestFile(fs, "empty.json", null);
    final URL emptyUrl = new URL(fs.getUri().toString() + manifest.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(emptyUrl.toString()));
    assertTrue(ex.getCause() instanceof UncheckedIOException);
    assertTrue(ex.getCause().getCause() instanceof EOFException);
    assertEquals("InputStream does not contain a valid ContextManifest at " + emptyUrl.toString(),
        ex.getCause().getCause().getMessage());
  }

  @Test
  public void testInitialInvalidJson() throws Exception {
    // Create a new manifest in HDFS, but with invalid content
    var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    // write out invalid json
    final var invalid =
        createContextManifestFile(fs, "invalid.json", manifest.toJson().substring(0, 4));
    final URL invalidUrl = new URL(fs.getUri().toString() + invalid.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(invalidUrl.toString()));
    assertTrue(ex.getCause() instanceof JsonSyntaxException);
    assertTrue(ex.getCause().getCause() instanceof EOFException);
  }

  @Test
  public void testInitial() throws Exception {
    var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var initial = createContextManifestFile(fs, "initial.json", manifest.toJson());
    final URL initialUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    ClassLoader cl = FACTORY.getClassLoader(initialUrl.toString());

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
    Files.copy(jarAPath, jarACopy, REPLACE_EXISTING);
    assertTrue(Files.exists(jarACopy));

    var manifest =
        ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarACopy.toUri().toURL());

    Files.delete(jarACopy);
    assertTrue(!Files.exists(jarACopy));

    final var initial = createContextManifestFile(fs, "missing-resource.json", manifest.toJson());
    final URL initialUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(initialUrl.toString()));
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
    resources.add(new Resource(jarAOrigLocation, "MD5", BAD_MD5));

    // remove the file:// prefix from the URL
    String goodJson = new ContextManifest(MONITOR_INTERVAL_SECS, resources).toJson();
    String badJson =
        goodJson.replace(jarAOrigLocation.toString(), jarAOrigLocation.toString().substring(6));
    assertNotEquals(goodJson, badJson);

    final var initial = createContextManifestFile(fs, "bad-resource-url.json", badJson);
    final URL initialUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(initialUrl.toString()));
    assertTrue(ex.getMessage().startsWith("Error getting classloader for context:"),
        ex::getMessage);
    assertTrue(ex.getCause() instanceof JsonSyntaxException);
    assertTrue(ex.getCause().getCause() instanceof MalformedURLException);
    assertTrue(ex.getCause().getCause().getMessage().startsWith("no protocol"),
        ex.getCause().getCause()::getMessage);
  }

  @Test
  public void testInitialBadResourceChecksum() throws Exception {
    Resource r = new Resource(jarAOrigLocation, "MD5", BAD_MD5);
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    resources.add(r);

    var manifest = new ContextManifest(MONITOR_INTERVAL_SECS, resources);

    final var initial =
        createContextManifestFile(fs, "bad-resource-checksum.json", manifest.toJson());
    final URL initialUrl = new URL(fs.getUri().toString() + initial.toUri().toString());

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(initialUrl.toString()));
    assertTrue(ex.getCause() instanceof IllegalStateException);
    assertTrue(ex.getCause().getMessage().startsWith("Error copying resource from file:"),
        ex::getMessage);
    assertTrue(ex.getCause().getCause() instanceof ExecutionException);
    assertTrue(ex.getCause().getCause().getCause() instanceof IllegalStateException);
    assertTrue(ex.getCause().getCause().getCause().getMessage().startsWith("Checksum"),
        ex.getCause().getCause().getCause()::getMessage);
    assertTrue(
        ex.getCause().getCause().getCause().getMessage()
            .endsWith("TestA.jar does not match checksum in the manifest " + BAD_MD5),
        ex.getCause().getCause().getCause()::getMessage);
  }

  @Test
  public void testUpdate() throws Exception {
    final var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var manifestPath = createContextManifestFile(fs, "update.json", manifest.toJson());
    final URL manifestUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(manifestUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // Update the contents
    var update = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarDOrigLocation);
    updateContextManifestFile(fs, manifestPath, update.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(manifestUrl.toString());

    assertNotEquals(cl, cl2);

    testClassFailsToLoad(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassLoads(cl2, classD);
  }

  @Test
  public void testUpdateSameClassNameDifferentContent() throws Exception {
    final var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var manifestPath =
        createContextManifestFile(fs, "update-same-name.json", manifest.toJson());
    final URL manifestUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(manifestUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // Update the contents
    var update = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarEOrigLocation);
    updateContextManifestFile(fs, manifestPath, update.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(manifestUrl.toString());

    assertNotEquals(cl, cl2);

    var clazz = cl2.loadClass(classA.getClassName()).asSubclass(test.Test.class);
    test.Test impl = clazz.getDeclaredConstructor().newInstance();
    assertEquals("Hello from E", impl.hello());
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);
  }

  @Test
  public void testUpdateManifestEmpty() throws Exception {
    final var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var manifestPath = createContextManifestFile(fs, "update-empty.json", manifest.toJson());
    final URL manifestUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(manifestUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // Update the contents with an empty file
    updateContextManifestFile(fs, manifestPath, null);

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(manifestUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);

  }

  @Test
  public void testUpdateNonExistentResource() throws Exception {
    final var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var manifestPath =
        createContextManifestFile(fs, "UpdateNonExistentResource.json", manifest.toJson());
    final URL manifestUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(manifestUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // copy jarA to jarACopy
    // create a manifest that references it
    // delete jarACopy
    var jarAPath = Path.of(jarAOrigLocation.toURI());
    var jarAPathParent = jarAPath.getParent();
    assertNotNull(jarAPathParent);
    var jarACopy = jarAPathParent.resolve("jarACopy.jar");
    assertTrue(!Files.exists(jarACopy));
    Files.copy(jarAPath, jarACopy, REPLACE_EXISTING);
    assertTrue(Files.exists(jarACopy));
    var manifest2 =
        ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarACopy.toUri().toURL());
    Files.delete(jarACopy);
    assertTrue(!Files.exists(jarACopy));

    updateContextManifestFile(fs, manifestPath, manifest2.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(manifestUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);
  }

  @Test
  public void testUpdateBadResourceChecksum() throws Exception {
    final var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var manifestPath =
        createContextManifestFile(fs, "UpdateBadResourceChecksum.json", manifest.toJson());
    final URL manifestUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(manifestUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    Resource r = new Resource(jarAOrigLocation, "MD5", BAD_MD5);
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    resources.add(r);

    var manifest2 = new ContextManifest(MONITOR_INTERVAL_SECS, resources);

    updateContextManifestFile(fs, manifestPath, manifest2.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(manifestUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);
  }

  @Test
  public void testUpdateBadResourceURL() throws Exception {
    final var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var manifestPath =
        createContextManifestFile(fs, "UpdateBadResourceChecksum.json", manifest.toJson());
    final URL manifestUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(manifestUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // remove the file:// prefix from the URL
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    resources.add(new Resource(jarAOrigLocation, "MD5", BAD_MD5));
    String goodJson = new ContextManifest(MONITOR_INTERVAL_SECS, resources).toJson();
    String badJson =
        goodJson.replace(jarAOrigLocation.toString(), jarAOrigLocation.toString().substring(6));
    assertNotEquals(goodJson, badJson);

    updateContextManifestFile(fs, manifestPath, badJson);

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(manifestUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);
  }

  @Test
  public void testUpdateInvalidJson() throws Exception {
    final var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var manifestPath =
        createContextManifestFile(fs, "update-invalid.json", manifest.toJson());
    final URL updateUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(updateUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    var update = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarDOrigLocation);
    updateContextManifestFile(fs, manifestPath, update.toJson().substring(0, 4));

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(updateUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);

    // Re-write the updated file such that it is now valid
    updateContextManifestFile(fs, manifestPath, update.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl3 = FACTORY.getClassLoader(updateUrl.toString());

    assertEquals(cl, cl2);
    assertNotEquals(cl, cl3);
    testClassFailsToLoad(cl3, classA);
    testClassFailsToLoad(cl3, classB);
    testClassFailsToLoad(cl3, classC);
    testClassLoads(cl3, classD);
  }

  @Test
  public void testChangingContext() throws Exception {
    var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation,
        jarBOrigLocation, jarCOrigLocation, jarDOrigLocation);
    final var manifestPath =
        createContextManifestFile(fs, "update-changing.json", manifest.toJson());
    final URL manifestUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(manifestUrl.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);

    final List<URL> allList = new ArrayList<>();
    allList.add(jarAOrigLocation);
    allList.add(jarBOrigLocation);
    allList.add(jarCOrigLocation);
    allList.add(jarDOrigLocation);

    List<URL> priorList = allList;
    ClassLoader priorCL = cl;

    for (int i = 0; i < 20; i++) {
      final List<URL> updatedList = new ArrayList<>(allList);
      Collections.shuffle(updatedList);
      final URL removed = updatedList.remove(0);

      // Update the contents
      var update =
          ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", updatedList.toArray(new URL[0]));
      updateContextManifestFile(fs, manifestPath, update.toJson());

      // wait 2x the monitor interval
      Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

      final ClassLoader updatedClassLoader = FACTORY.getClassLoader(manifestUrl.toString());

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
    final var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation);
    final var manifestPath =
        createContextManifestFile(fs, "UpdateNonExistentResource.json", manifest.toJson());
    final URL manifestUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(manifestUrl.toString());

    testClassLoads(cl, classA);
    testClassFailsToLoad(cl, classB);
    testClassFailsToLoad(cl, classC);
    testClassFailsToLoad(cl, classD);

    // copy jarA to jarACopy
    // create a manifest that references it
    // delete jarACopy
    var jarAPath = Path.of(jarAOrigLocation.toURI());
    var jarAPathParent = jarAPath.getParent();
    assertNotNull(jarAPathParent);
    var jarACopy = jarAPathParent.resolve("jarACopy.jar");
    assertTrue(!Files.exists(jarACopy));
    Files.copy(jarAPath, jarACopy, REPLACE_EXISTING);
    assertTrue(Files.exists(jarACopy));
    var manifest2 =
        ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarACopy.toUri().toURL());
    Files.delete(jarACopy);
    assertTrue(!Files.exists(jarACopy));

    updateContextManifestFile(fs, manifestPath, manifest2.toJson());

    // wait 2x the monitor interval
    Thread.sleep(MONITOR_INTERVAL_SECS * 2 * 1000);

    final ClassLoader cl2 = FACTORY.getClassLoader(manifestUrl.toString());

    // validate that the classloader has not updated
    assertEquals(cl, cl2);
    testClassLoads(cl2, classA);
    testClassFailsToLoad(cl2, classB);
    testClassFailsToLoad(cl2, classC);
    testClassFailsToLoad(cl2, classD);

    // Wait 2 minutes for grace period to expire
    Thread.sleep(120_000);

    var ex = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(manifestUrl.toString()));
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
    var manifest = ContextManifest.create(MONITOR_INTERVAL_SECS, "SHA-512", jarAOrigLocation,
        jarBOrigLocation, jarCOrigLocation, jarDOrigLocation);
    final var manifestPath =
        createContextManifestFile(fs, "update-external-modified.json", manifest.toJson());
    final URL manifestUrl = new URL(fs.getUri().toString() + manifestPath.toUri().toString());

    final ClassLoader cl = FACTORY.getClassLoader(manifestUrl.toString());
    testClassLoads(cl, classA);
    testClassLoads(cl, classB);
    testClassLoads(cl, classC);
    testClassLoads(cl, classD);

    var resources = baseCacheDir.resolve(RESOURCES_DIR);
    List<Path> files = manifest.getResources().stream()
        .map(r -> resources.resolve(LocalStore.localResourceName(r))).limit(2)
        .collect(Collectors.toList());
    assertEquals(2, files.size());

    // overwrite one downloaded jar with others content
    Files.copy(files.get(0), files.get(1), REPLACE_EXISTING);

    final var update2 =
        createContextManifestFile(fs, "update-external-modified2.json", manifest.toJson());
    final URL updateUrl2 = new URL(fs.getUri().toString() + update2.toUri().toString());

    // The classloader should fail to create because one of the files in the local filesystem cache
    // has a checksum mismatch
    var exception = assertThrows(ContextClassLoaderException.class,
        () -> FACTORY.getClassLoader(updateUrl2.toString()));
    assertTrue(exception.getMessage().contains("Checksum"), exception::getMessage);

    // clean up corrupt file
    Files.delete(files.get(1));

    // ensure it works now
    FACTORY.getClassLoader(updateUrl2.toString());
  }

  @Test
  public void testConcurrentDeletes() throws Exception {

    final var executor = Executors.newCachedThreadPool();
    final var stop = new AtomicBoolean(false);

    // create a background task that continually concurrently deletes files in the resources dir
    var deleteFuture = executor.submit(() -> {
      while (!stop.get()) {
        var resourcesDir = baseCacheDir.resolve(RESOURCES_DIR).toFile();
        assertTrue(resourcesDir.exists() && resourcesDir.isDirectory());
        var files = resourcesDir.listFiles();
        for (var file : files) {
          assertTrue(file.delete());
        }
        Thread.sleep(100);
      }
      return null;
    });

    var manifest = ContextManifest.create(100, "SHA-512", jarAOrigLocation, jarBOrigLocation,
        jarCOrigLocation, jarDOrigLocation);

    List<Future<?>> futures = new ArrayList<>();

    // create 10 threads that are continually creating new classloaders, the deletes should cause
    // hard link creations to fail sometimes
    for (int i = 0; i < 10; i++) {
      int threadNum = i;
      var future = executor.submit(() -> {
        Timer timer = Timer.startNew();
        int j = 0;
        ClassLoader lastCl = null;
        while (!timer.hasElapsed(10, TimeUnit.SECONDS)) {
          var file = tempDir.resolve("context-cd-" + threadNum + "_" + j + ".json");
          Files.writeString(file, manifest.toJson());
          var url = file.toUri().toURL().toExternalForm();

          final ClassLoader cl = FACTORY.getClassLoader(url);
          // This test is assuming that each call above creates a new classloader which in turn
          // creates new hard links. This is checking that assumption in case the impl changes and
          // this test needs to be reevaluated.
          assertNotSame(cl, lastCl);
          lastCl = cl;
          testClassLoads(cl, classA);
          testClassLoads(cl, classB);
          testClassLoads(cl, classC);
          testClassLoads(cl, classD);
          j++;
        }

        return null;
      });
      futures.add(future);
    }

    for (var future : futures) {
      future.get();
    }

    stop.set(true);
    // ensure the delete task had no errors
    deleteFuture.get();
    executor.shutdown();

    long workingDirsCount =
        Files.list(baseCacheDir.resolve(WORKING_DIR)).filter(p -> p.toFile().isDirectory()).count();
    // check that many hard link directories were created
    assertTrue(workingDirsCount > 50);
  }
}
