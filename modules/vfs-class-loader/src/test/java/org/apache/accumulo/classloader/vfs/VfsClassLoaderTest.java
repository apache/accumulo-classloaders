/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.classloader.vfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VfsClassLoaderTest extends AccumuloDFSBase {

  private static final Logger LOG = LoggerFactory.getLogger(VfsClassLoaderTest.class);
  private static final Path TEST_DIR = new Path(getHdfsUri() + "/test-dir");

  private static FileSystem hdfs = null;
  private static DefaultFileSystemManager vfs = null;

  @BeforeClass
  public static void setup() throws Exception {

    // miniDfsClusterSetup();

    hdfs = getCluster().getFileSystem();
    assertTrue("Unable to create: " + TEST_DIR, hdfs.mkdirs(TEST_DIR));

    vfs = getDefaultFileSystemManager();

  }

  @Before
  public void before() throws Exception {
    // Copy jar file to TEST_DIR
    URL jarPath = VfsClassLoaderTest.class.getResource("/HelloWorld.jar");
    assertNotNull("Unable to find HelloWorld.jar", jarPath);
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, src.getName());
    LOG.info("Copying {} to {}", src, dst);
    hdfs.copyFromLocalFile(src, dst);
  }

  @Test
  public void testGetClass() throws Exception {

    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    assertNotNull("Unable to resolve test dir via VFS", testDir);
    FileObject[] dirContents = testDir.getChildren();
    LOG.info("Test directory contents according to VFS: {}", Arrays.toString(dirContents));

    VFSClassLoader cl = AccessController.doPrivileged(new PrivilegedAction<VFSClassLoader>() {
      @Override
      public VFSClassLoader run() {
        // Point the VFSClassLoader to all of the objects in TEST_DIR
        try {
          return new VFSClassLoader(dirContents, vfs);
        } catch (FileSystemException e) {
          throw new RuntimeException("Error creating VFSClassLoader", e);
        }
      }
    });

    LOG.info("VFSClassLoader has the following files: {}", Arrays.toString(cl.getFileObjects()));
    LOG.info("Looking for HelloWorld.class");
    Class<?> helloWorldClass = cl.loadClass("test.HelloWorld");
    Object o = helloWorldClass.getDeclaredConstructor().newInstance();
    assertEquals("Hello World!", o.toString());
  }

  @Test
  public void testFileMonitor() throws Exception {
    MyFileMonitor listener = new MyFileMonitor();
    DefaultFileMonitor monitor = new DefaultFileMonitor(listener);
    monitor.setRecursive(true);
    FileObject testDir = vfs.resolveFile(TEST_DIR.toUri().toString());
    monitor.addFile(testDir);
    monitor.start();

    // Copy jar file to a new file name
    URL jarPath = this.getClass().getResource("/HelloWorld.jar");
    Path src = new Path(jarPath.toURI().toString());
    Path dst = new Path(TEST_DIR, "HelloWorld2.jar");
    hdfs.copyFromLocalFile(src, dst);

    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);
    assertTrue(listener.isFileCreated());

    // Update the jar
    jarPath = this.getClass().getResource("/HelloWorld.jar");
    src = new Path(jarPath.toURI().toString());
    dst = new Path(TEST_DIR, "HelloWorld2.jar");
    hdfs.copyFromLocalFile(src, dst);

    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);
    assertTrue(listener.isFileChanged());

    hdfs.delete(dst, false);
    // VFS-487 significantly wait to avoid failure
    Thread.sleep(7000);
    assertTrue(listener.isFileDeleted());

    monitor.stop();

  }

  @AfterClass
  public static void tearDown() throws Exception {
    hdfs.delete(TEST_DIR, true);
    tearDownMiniDfsCluster();
  }

  public static class MyFileMonitor implements FileListener {

    private boolean fileChanged = false;
    private boolean fileDeleted = false;
    private boolean fileCreated = false;

    @Override
    public void fileCreated(FileChangeEvent event) throws Exception {
      // System.out.println(event.getFile() + " created");
      this.fileCreated = true;
    }

    @Override
    public void fileDeleted(FileChangeEvent event) throws Exception {
      // System.out.println(event.getFile() + " deleted");
      this.fileDeleted = true;
    }

    @Override
    public void fileChanged(FileChangeEvent event) throws Exception {
      // System.out.println(event.getFile() + " changed");
      this.fileChanged = true;
    }

    public boolean isFileChanged() {
      return fileChanged;
    }

    public boolean isFileDeleted() {
      return fileDeleted;
    }

    public boolean isFileCreated() {
      return fileCreated;
    }

  }
}
