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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.classloader.hdfs.HDFSContextClassLoaderFactory.Context;
import org.apache.accumulo.classloader.hdfs.HDFSContextClassLoaderFactory.JarInfo;
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

    Path HDFSContext1Path = new Path(HDFS_CONTEXT1_DIR);
    hdfs.mkdirs(HDFSContext1Path, FsPermission.getDirDefault());
    LOG.debug("Created dir(s) " + HDFSContext1Path);

    System.setProperty(HDFSContextClassLoaderFactory.HDFS_CONTEXTS_BASE_DIR,
        hdfs.getUri() + HDFS_CONTEXTS_DIR);
    System.setProperty(HDFSContextClassLoaderFactory.LOCAL_CONTEXTS_DOWNLOAD_DIR, localContextsDir);
    System.setProperty(HDFSContextClassLoaderFactory.MANIFEST_FILE_CHECK_INTERVAL,
        CHECK_INTERVAL_SEC + "");
  }

  @AfterEach
  public void teardown() throws Exception {
    hdfsCluster.close();
    hdfsCluster.shutdown(true);
    FileUtils.deleteDirectory(
        new File(System.getProperty(HDFSContextClassLoaderFactory.LOCAL_CONTEXTS_DOWNLOAD_DIR)));
  }

  private void writeManifestFileToHDFS(Context context) throws Exception {
    Path HDFSManifestPath = new Path(HDFS_MANIFEST_FILE);
    try (var os = hdfs.create(HDFSManifestPath)) {
      Gson gson = new Gson().newBuilder().setPrettyPrinting().create();
      os.write(gson.toJson(context).getBytes(StandardCharsets.UTF_8));
      LOG.debug("Wrote {}{}{} to {}", System.lineSeparator(), gson.toJson(context),
          System.lineSeparator(), HDFSManifestPath);
    }
  }

  private void writeJarFileToHDFS(Path HDFSJarPath, String resourceName) throws Exception {
    Path localJar = new Path(this.getClass().getResource(resourceName).toURI());
    hdfs.copyFromLocalFile(localJar, HDFSJarPath);
    LOG.debug("Copied from {} to {}", localJar, HDFSJarPath);
  }

  private void writeManfiestAndJarToHDFS(String jarName, String resourceName, String contextName,
      boolean validChecksum) throws Exception {
    var HDFSJarPath = new Path(HDFS_CONTEXT1_DIR, jarName);
    writeJarFileToHDFS(HDFSJarPath, resourceName);

    final JarInfo jarInfo;
    if (validChecksum) {
      jarInfo = new JarInfo(jarName,
          HDFSContextClassLoaderFactory.checksum(hdfs.open(HDFSJarPath).readAllBytes()));
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

    writeManfiestAndJarToHDFS(HELLO_WORLD_JAR, HELLO_WORLD_JAR_RESOURCE_NAME, CONTEXT1, true);

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

    writeManfiestAndJarToHDFS(HELLO_WORLD_JAR, HELLO_WORLD_JAR_RESOURCE_NAME, CONTEXT1, true);

    var classLoaderA = factory.getClassLoader(CONTEXT1);
    assertThrows(ClassNotFoundException.class, () -> classLoaderA.loadClass(TEST_CLASS));
    var clazz = classLoaderA.loadClass(HELLO_WORLD_CLASS);
    var methods = clazz.getMethods();
    assertEquals(1,
        Arrays.stream(methods).filter(method -> method.getName().equals("validate")).count());

    // note that we are writing the manifest file with the same context name but without the
    // hello world jar but with the test jar
    writeManfiestAndJarToHDFS(TEST_JAR, TEST_JAR_RESOURCE_NAME, CONTEXT1, true);

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

    writeManfiestAndJarToHDFS(HELLO_WORLD_JAR, HELLO_WORLD_JAR_RESOURCE_NAME, CONTEXT1, false);

    assertThrows(ContextClassLoaderFactory.ContextClassLoaderException.class,
        () -> factory.getClassLoader(CONTEXT1));
  }

  /**
   * Tests many threads running calls to
   * {@link HDFSContextClassLoaderFactory#getClassLoader(String)} and many threads running the
   * {@link HDFSContextClassLoaderFactory.ManifestFileChecker}. This is to test that there are no
   * race conditions for shared resources of this factory (the file systems and the internal store
   * for the class loaders). In the implementation, just one
   * {@link HDFSContextClassLoaderFactory.ManifestFileChecker} runs, but this tests many of these
   * running to more thoroughly test for race conditions. This test will also change the manifest
   * file for the context to reference a new jar, which should be picked up by a checker and cause
   * the context entry in the internal store to be updated.
   */
  @Test
  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "predictable random fine for testing")
  public void testConcurrent() throws Exception {
    // have the manifest file checks run more often
    final int manifestFileCheckIntervalSec = 1;
    System.setProperty(HDFSContextClassLoaderFactory.MANIFEST_FILE_CHECK_INTERVAL,
        manifestFileCheckIntervalSec + "");
    final int numTotalThreads = 128;
    final int numClassLoaderThreads = numTotalThreads / 2;
    final int numManifestFileCheckerThreads = numTotalThreads / 2;
    final Random rand = new Random();
    ExecutorService classLoaderPool = Executors.newFixedThreadPool(numClassLoaderThreads);
    ExecutorService manifestFileCheckerPool =
        Executors.newFixedThreadPool(numManifestFileCheckerThreads);

    try {
      CountDownLatch latch = new CountDownLatch(numTotalThreads);
      List<Future<ClassLoader>> classLoaderFutures = new ArrayList<>(numClassLoaderThreads);
      List<Future<?>> manifestFileCheckerFutures = new ArrayList<>(numManifestFileCheckerThreads);
      Set<ClassLoader> classLoaderPoolResults = new HashSet<>();
      HDFSContextClassLoaderFactory factory = new HDFSContextClassLoaderFactory();

      factory.init(null);

      // context1 -> HelloWorld jar
      writeManfiestAndJarToHDFS(HELLO_WORLD_JAR, HELLO_WORLD_JAR_RESOURCE_NAME, CONTEXT1, true);

      for (int i = 0; i < numClassLoaderThreads; i++) {
        classLoaderFutures.add(classLoaderPool.submit(() -> {
          try {
            latch.countDown();
            latch.await();
            // make ~1/2 the threads race to call getClassLoader immediately (racing to get the
            // class loader for the HelloWorld jar), make the other ~1/2 race to call after
            // the manifest file change occurs and would be picked up (racing to get the class
            // loader for the Test jar)
            if (rand.nextInt(2) == 0) {
              Thread.sleep(TimeUnit.SECONDS.toMillis(manifestFileCheckIntervalSec + 1));
            }
            var classLoader = factory.getClassLoader(CONTEXT1);
            return classLoader;
          } catch (ContextClassLoaderFactory.ContextClassLoaderException e) {
            throw new RuntimeException(e);
          }
        }));
      }

      for (int i = 0; i < numManifestFileCheckerThreads; i++) {
        manifestFileCheckerFutures.add(manifestFileCheckerPool.submit(() -> {
          latch.countDown();
          try {
            latch.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          factory.new ManifestFileChecker().run();
        }));
      }

      // context1 -> Test jar
      writeManfiestAndJarToHDFS(TEST_JAR, TEST_JAR_RESOURCE_NAME, CONTEXT1, true);

      for (var future : classLoaderFutures) {
        classLoaderPoolResults.add(future.get());
      }
      factory.shutdownManifestFileChecker();
      for (var future : manifestFileCheckerFutures) {
        future.get();
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
      classLoaderPool.shutdown();
      manifestFileCheckerPool.shutdown();
      System.setProperty(HDFSContextClassLoaderFactory.MANIFEST_FILE_CHECK_INTERVAL,
          CHECK_INTERVAL_SEC + "");
    }
  }
}
