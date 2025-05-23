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
package org.apache.accumulo.classloader.vfs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.File;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class AccumuloVFSClassLoaderTest {

  @TempDir
  private static File tempDir;
  String folderPath;

  @BeforeEach
  public void setup() throws Exception {
    System.setProperty(AccumuloVFSClassLoader.VFS_CLASSPATH_MONITOR_INTERVAL, "1");
    VFSManager.initialize();

    folderPath = tempDir.toURI() + ".*";

    FileUtils.copyURLToFile(Objects.requireNonNull(this.getClass().getResource("/HelloWorld.jar")),
        new File(tempDir, "HelloWorld.jar"));
  }

  FileObject[] createFileSystems(FileObject[] fos) throws FileSystemException {
    FileObject[] rfos = new FileObject[fos.length];
    for (int i = 0; i < fos.length; i++) {
      if (VFSManager.get().canCreateFileSystem(fos[i])) {
        rfos[i] = VFSManager.get().createFileSystem(fos[i]);
      } else {
        rfos[i] = fos[i];
      }
    }

    return rfos;
  }

  @Test
  public void testConstructor() throws Exception {
    FileObject testDir = VFSManager.get().resolveFile(tempDir.toURI().toString());
    FileObject[] dirContents = testDir.getChildren();

    AccumuloVFSClassLoader arvcl = new AccumuloVFSClassLoader(ClassLoader.getSystemClassLoader()) {
      @Override
      protected String getClassPath() {
        return folderPath;
      }
    };
    arvcl.setVMInitializedForTests();

    FileObject[] files = ((VFSClassLoaderWrapper) arvcl.getDelegateClassLoader()).getFileObjects();
    assertArrayEquals(createFileSystems(dirContents), files);

    arvcl.close();
  }

}
