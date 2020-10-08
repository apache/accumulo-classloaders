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

import static org.junit.Assert.assertArrayEquals;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class AccumuloVFSClassLoaderTest {

  @Rule
  public TemporaryFolder folder1 =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));
  String folderPath;
  private DefaultFileSystemManager vfs;

  @Before
  public void setup() throws Exception {
    System.setProperty(AccumuloVFSClassLoader.VFS_CLASSPATH_MONITOR_INTERVAL, "1");
    vfs = VFSManager.generateVfs();

    folderPath = folder1.getRoot().toURI() + ".*";

    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        folder1.newFile("HelloWorld.jar"));
  }

  FileObject[] createFileSystems(FileObject[] fos) throws FileSystemException {
    FileObject[] rfos = new FileObject[fos.length];
    for (int i = 0; i < fos.length; i++) {
      if (vfs.canCreateFileSystem(fos[i])) {
        rfos[i] = vfs.createFileSystem(fos[i]);
      } else {
        rfos[i] = fos[i];
      }
    }

    return rfos;
  }

  @Test
  public void testConstructor() throws Exception {
    FileObject testDir = vfs.resolveFile(folder1.getRoot().toURI().toString());
    FileObject[] dirContents = testDir.getChildren();

    AccumuloVFSClassLoader arvcl = new AccumuloVFSClassLoader(ClassLoader.getSystemClassLoader()) {
      @Override
      protected String getClassPath() {
        return folderPath;
      }

      @Override
      protected DefaultFileSystemManager getFileSystem() {
        return vfs;
      }
    };
    arvcl.setVMInitializedForTests();
    arvcl.setVFSForTests(vfs);

    FileObject[] files = ((VFSClassLoaderWrapper) arvcl.getDelegateClassLoader()).getFileObjects();
    assertArrayEquals(createFileSystems(dirContents), files);

    arvcl.close();
  }

}
