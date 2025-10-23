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
package org.apache.accumulo.classloader.hdfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.accumulo.classloader.hdfs.HDFSContextClassLoaderFactory.Context;
import org.apache.accumulo.classloader.hdfs.HDFSContextClassLoaderFactory.JarInfo;
import org.apache.accumulo.classloader.hdfs.HDFSContextClassLoaderFactory.StoreCleaner;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class HDFSContextClassLoaderFactoryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(HDFSContextClassLoaderFactoryTest.class);
  private static final String CONTEXT1 = "context1";
  private static final String HELLO_WORLD_JAR = "HelloWorld.jar";
  private static final String HELLO_WORLD_JAR_RESOURCE_NAME = "/" + HELLO_WORLD_JAR;
  private static final String HELLO_WORLD_CLASS = "test.HelloWorld";
  private static final String TEST_JAR = "Test.jar";
  private static final String TEST_JAR_RESOURCE_NAME = "/ClassLoaderTestA/" + TEST_JAR;
  private static final String TEST_CLASS = "test.TestObject";
  private static final String HDFS_BASE_DIR = "/hdfs";
  private static final String HDFS_CONTEXTS_DIR = HDFS_BASE_DIR + "/contexts";
  private static final String HDFS_CONTEXT1_DIR = HDFS_CONTEXTS_DIR + "/" + CONTEXT1;
  private static final String HDFS_MANIFEST_FILE =
      HDFS_CONTEXT1_DIR + "/" + HDFSContextClassLoaderFactory.MANIFEST_FILE_NAME;
  private static final long CHECK_INTERVAL_SEC = 5;
  final String localContextsDir = System.getProperty("user.dir") + "/target/contexts";
  private MiniDFSCluster hdfsCluster;
  private FileSystem hdfs;

  @BeforeEach
  public void setup() throws Exception {
    Configuration hadoopConf = new Configuration();
    hdfsCluster = new MiniDFSCluster.Builder(hadoopConf).build();
    hdfsCluster.waitClusterUp();
    hdfs = hdfsCluster.getFileSystem();

    Path hdfsContext1Path = new Path(HDFS_CONTEXT1_DIR);
    hdfs.mkdirs(hdfsContext1Path, FsPermission.getDirDefault());
    LOG.debug("Created dir(s) " + hdfsContext1Path);

    System.setProperty(HDFSContextClassLoaderFactory.HDFS_CONTEXTS_BASE_DIR,
        hdfs.getUri() + HDFS_CONTEXTS_DIR);
    System.setProperty(HDFSContextClassLoaderFactory.LOCAL_CONTEXTS_DOWNLOAD_DIR, localContextsDir);
  }

  @AfterEach
  public void teardown() throws Exception {
    hdfsCluster.close();
    hdfsCluster.shutdown(true);
    FileUtils.deleteDirectory(
        new File(System.getProperty(HDFSContextClassLoaderFactory.LOCAL_CONTEXTS_DOWNLOAD_DIR)));
  }

  private void writeManifestFileToHDFS(Context context) throws Exception {
    Path hdfsManifestPath = new Path(HDFS_MANIFEST_FILE);
    try (var os = hdfs.create(hdfsManifestPath)) {
      Gson gson = new Gson().newBuilder().setPrettyPrinting().create();
      os.write(gson.toJson(context).getBytes(StandardCharsets.UTF_8));
      LOG.debug("Wrote {}{}{} to {}", System.lineSeparator(), gson.toJson(context),
          System.lineSeparator(), hdfsManifestPath);
    }
  }

  private void writeJarFileToHDFS(Path hdfsJarPath, String resourceName) throws Exception {
    Path localJar = new Path(this.getClass().getResource(resourceName).toURI());
    hdfs.copyFromLocalFile(localJar, hdfsJarPath);
    LOG.debug("Copied from {} to {}", localJar, hdfsJarPath);
  }

  private void writeManifestAndJarToHDFS(HDFSContextClassLoaderFactory factory, String jarName,
      String resourceName, String contextName, boolean validChecksum) throws Exception {
    var hdfsJarPath = new Path(HDFS_CONTEXT1_DIR, jarName);
    writeJarFileToHDFS(hdfsJarPath, resourceName);

    final JarInfo jarInfo;
    if (validChecksum) {
      jarInfo = new JarInfo(jarName, factory.checksum(hdfs.open(hdfsJarPath)));
    } else {
      jarInfo = new JarInfo(jarName, "badchecksum");
    }
    final Context context = new Context(contextName, jarInfo);

    writeManifestFileToHDFS(context);
  }

  @Test
  public void testBasic() throws Exception {
    HDFSContextClassLoaderFactory factory = new HDFSContextClassLoaderFactory();
    factory.init(null);

    writeManifestAndJarToHDFS(factory, HELLO_WORLD_JAR, HELLO_WORLD_JAR_RESOURCE_NAME, CONTEXT1,
        true);

    var classLoader = factory.getClassLoader(CONTEXT1);
    var clazz = classLoader.loadClass(HELLO_WORLD_CLASS);
    var methods = clazz.getMethods();
    assertEquals(1,
        Arrays.stream(methods).filter(method -> method.getName().equals("validate")).count());
  }

  @Test
  public void testChangingManifest() throws Exception {
    HDFSContextClassLoaderFactory factory = new HDFSContextClassLoaderFactory();
    factory.init(null);

    writeManifestAndJarToHDFS(factory, HELLO_WORLD_JAR, HELLO_WORLD_JAR_RESOURCE_NAME, CONTEXT1,
        true);

    var classLoaderA = factory.getClassLoader(CONTEXT1);
    assertThrows(ClassNotFoundException.class, () -> classLoaderA.loadClass(TEST_CLASS));
    var clazz = classLoaderA.loadClass(HELLO_WORLD_CLASS);
    var methods = clazz.getMethods();
    assertEquals(1,
        Arrays.stream(methods).filter(method -> method.getName().equals("validate")).count());

    // note that we are writing the manifest file with the same context name but without the
    // hello world jar but with the test jar
    writeManifestAndJarToHDFS(factory, TEST_JAR, TEST_JAR_RESOURCE_NAME, CONTEXT1, true);

    // wait for manifest file check to take place
    Thread.sleep(TimeUnit.SECONDS.toMillis(CHECK_INTERVAL_SEC + 1));

    var classLoaderB = factory.getClassLoader(CONTEXT1);
    assertThrows(ClassNotFoundException.class, () -> classLoaderB.loadClass(HELLO_WORLD_CLASS));
    clazz = classLoaderB.loadClass(TEST_CLASS);
    methods = clazz.getMethods();
    assertEquals(1,
        Arrays.stream(methods).filter(method -> method.getName().equals("add")).count());
  }

  @Test
  public void testIncorrectChecksum() throws Exception {
    HDFSContextClassLoaderFactory factory = new HDFSContextClassLoaderFactory();
    factory.init(null);

    writeManifestAndJarToHDFS(factory, HELLO_WORLD_JAR, HELLO_WORLD_JAR_RESOURCE_NAME, CONTEXT1,
        false);

    assertThrows(ContextClassLoaderFactory.ContextClassLoaderException.class,
        () -> factory.getClassLoader(CONTEXT1));
  }

  /**
   * Tests many threads running calls to
   * {@link HDFSContextClassLoaderFactory#getClassLoader(String)}. This is to test for race
   * conditions for shared resources of this factory (the file systems and the internal store for
   * the class loaders).
   */
  @Test
  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "predictable random fine for testing")
  public void testConcurrent() throws Exception {
    HDFSContextClassLoaderFactory factory = new HDFSContextClassLoaderFactory();
    factory.init(null);

    int numThreads = 128;
    ExecutorService classLoaderPool = Executors.newFixedThreadPool(numThreads);
    // for this test, we don't expect anything to be GC'd or removed from the store. Run these
    // to confirm this assumption.
    GCRunner gcRunner = new GCRunner();
    // use our own cleaner to run the cleans more often than the factory does, increasing
    // concurrency stress
    StoreCleaner storeCleaner = factory.new StoreCleaner(10);

    try {
      Thread gcThread = new Thread(gcRunner);
      gcThread.start();
      Thread storeCleanerThread = new Thread(storeCleaner);
      storeCleanerThread.start();

      CountDownLatch latch1 = new CountDownLatch(numThreads / 2);
      CountDownLatch latch2 = new CountDownLatch(numThreads / 2);
      List<Future<ClassLoader>> classLoaderFutures = new ArrayList<>(numThreads / 2);
      Set<ClassLoader> classLoaderPoolResults = new HashSet<>();

      // context1 -> HelloWorld jar
      writeManifestAndJarToHDFS(factory, HELLO_WORLD_JAR, HELLO_WORLD_JAR_RESOURCE_NAME, CONTEXT1,
          true);

      for (int i = 0; i < numThreads / 2; i++) {
        classLoaderFutures.add(classLoaderPool.submit(() -> {
          try {
            latch1.countDown();
            latch1.await();
            // make 1/2 the threads race to getClassLoader for the HelloWorld jar
            return factory.getClassLoader(CONTEXT1);
          } catch (ContextClassLoaderFactory.ContextClassLoaderException e) {
            throw new RuntimeException(e);
          }
        }));
      }
      // wait for threads to complete
      for (var future : classLoaderFutures) {
        classLoaderPoolResults.add(future.get());
      }
      classLoaderFutures.clear();

      // context1 -> Test jar
      writeManifestAndJarToHDFS(factory, TEST_JAR, TEST_JAR_RESOURCE_NAME, CONTEXT1, true);

      for (int i = 0; i < numThreads / 2; i++) {
        classLoaderFutures.add(classLoaderPool.submit(() -> {
          try {
            latch2.countDown();
            latch2.await();
            // make 1/2 the threads race to getClassLoader for the Test jar
            return factory.getClassLoader(CONTEXT1);
          } catch (ContextClassLoaderFactory.ContextClassLoaderException e) {
            throw new RuntimeException(e);
          }
        }));
      }
      // wait for threads to complete
      for (var future : classLoaderFutures) {
        classLoaderPoolResults.add(future.get());
      }

      // only two unique class loaders should have been created
      assertEquals(2, classLoaderPoolResults.size());
      for (var classLoader : classLoaderPoolResults) {
        var classLoaderUrls = ((URLClassLoader) classLoader).getURLs();
        assertEquals(1, classLoaderUrls.length);
        if (classLoaderUrls[0].getFile().contains(HELLO_WORLD_JAR)) {
          // should not be able to load any classes for this jar: jar should no longer exist
          // (context was changed to only include other jar)
          assertThrows(ClassNotFoundException.class,
              () -> classLoader.loadClass(HELLO_WORLD_CLASS));
          assertThrows(ClassNotFoundException.class, () -> classLoader.loadClass(TEST_CLASS));
        } else if (classLoaderUrls[0].getFile().contains(TEST_JAR)) {
          assertThrows(ClassNotFoundException.class,
              () -> classLoader.loadClass(HELLO_WORLD_CLASS));
          var testClass = classLoader.loadClass(TEST_CLASS);
          var testMethods = testClass.getMethods();
          assertEquals(1,
              Arrays.stream(testMethods).filter(method -> method.getName().equals("add")).count());
        }
      }

      // only one directory should exist in the local contexts directory, and it should be the
      // context1 dir with the Test jar (no temp dirs should exist)
      try (var contextsDirChildren = Files.list(java.nio.file.Path.of(localContextsDir))) {
        Set<java.nio.file.Path> pathsSeen = contextsDirChildren.collect(Collectors.toSet());
        LOG.debug("Paths seen contextsDirChildren " + pathsSeen);
        assertEquals(1, pathsSeen.size());
        var contextPath = java.nio.file.Path.of(localContextsDir, CONTEXT1);
        assertEquals(contextPath, pathsSeen.iterator().next());
        try (var contextDirChildren = Files.list(contextPath)) {
          pathsSeen = contextDirChildren.collect(Collectors.toSet());
          LOG.debug("Paths seen contextDirChildren " + pathsSeen);
          // The jar itself and the Hadoop CRC file for the jar
          assertEquals(2, pathsSeen.size());
          for (var pathSeen : pathsSeen) {
            var fileName = pathSeen.getFileName();
            assertNotNull(fileName);
            assertTrue(
                fileName.toString().contains(".crc") || fileName.toString().equals(TEST_JAR));
          }
        }
      }
    } finally {
      factory.shutdownStoreCleaner();
      gcRunner.shutdown();
      classLoaderPool.shutdown();
    }
  }

  /**
   * Tests that stored values are GC'd when no longer needed
   */
  @Test
  public void testStoreCleanup() throws Exception {
    HDFSContextClassLoaderFactory factory = new HDFSContextClassLoaderFactory();
    factory.init(null);

    GCRunner gcRunner = new GCRunner();
    StoreCleaner storeCleaner = factory.new StoreCleaner(10);
    try {
      Thread gcThread = new Thread(gcRunner);
      gcThread.start();
      // use our own store cleaner to run the cleans more often than the factory does, reducing
      // wait times
      Thread storeCleanerThread = new Thread(storeCleaner);
      storeCleanerThread.start();

      writeManifestAndJarToHDFS(factory, HELLO_WORLD_JAR, HELLO_WORLD_JAR_RESOURCE_NAME, CONTEXT1,
          true);

      // create a strong reference to the class loader
      var classLoader = factory.getClassLoader(CONTEXT1);
      // wait a bit allowing GCs and store cleans to run
      Thread.sleep(1_000);
      assertFalse(factory.classLoaders.isEmpty());
      try (var contextsDirChildren = Files.list(java.nio.file.Path.of(localContextsDir))) {
        assertEquals(1, contextsDirChildren.count());
      }

      // clear strong reference to class loader
      classLoader = null;
      // wait a bit allowing GCs and store cleans to run
      Thread.sleep(1_000);
      assertTrue(factory.classLoaders.isEmpty());
      try (var contextsDirChildren = Files.list(java.nio.file.Path.of(localContextsDir))) {
        assertEquals(0, contextsDirChildren.count());
      }
    } finally {
      factory.shutdownStoreCleaner();
      gcRunner.shutdown();
    }
  }

  /**
   * Since our factory works with weak references, have a thread to run GC very often to ensure
   * things are GC'd only when expected
   */
  private static class GCRunner implements Runnable {
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    @Override
    public void run() {
      while (!shutdown.get()) {
        System.gc();
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private void shutdown() {
      shutdown.set(true);
    }
  }
}
