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
package org.apache.accumulo.classloader.vfs.context;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.classloader.vfs.context.ReloadingVFSContextClassLoaderFactory.Context;
import org.apache.accumulo.classloader.vfs.context.ReloadingVFSContextClassLoaderFactory.ContextConfig;
import org.apache.accumulo.classloader.vfs.context.ReloadingVFSContextClassLoaderFactory.Contexts;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class ReloadingVFSContextClassLoaderFactoryTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReloadingVFSContextClassLoaderFactoryTest.class);

  private static class TestReloadingVFSContextClassLoaderFactory
      extends ReloadingVFSContextClassLoaderFactory {

    private final String dir;

    public TestReloadingVFSContextClassLoaderFactory(String dir) {
      this.dir = dir;
    }

    @Override
    protected AccumuloVFSClassLoader create(Context c) {
      AccumuloVFSClassLoader acl =
          AccessController.doPrivileged(new PrivilegedAction<AccumuloVFSClassLoader>() {
            @Override
            public AccumuloVFSClassLoader run() {
              AccumuloVFSClassLoader cl = new AccumuloVFSClassLoader(
                  ReloadingVFSContextClassLoaderFactory.class.getClassLoader()) {
                @Override
                protected String getClassPath() {
                  return dir;
                }

                @Override
                protected boolean isPostDelegationModel() {
                  LOG.debug("isPostDelegationModel called, returning {}",
                      c.getConfig().getPostDelegate());
                  return c.getConfig().getPostDelegate();
                }

                @Override
                protected long getMonitorInterval() {
                  return 500l;
                }

                @Override
                protected boolean isVMInitialized() {
                  return true;
                }
              };
              cl.setVMInitializedForTests();
              cl.setMaxRetries(2);
              return cl;
            }

          });
      return acl;
    }
  }

  @Rule
  public TemporaryFolder TEMP =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private static final Contexts c = new Contexts();

  private static File foo = new File(System.getProperty("user.dir") + "/target/foo");
  private static File bar = new File(System.getProperty("user.dir") + "/target/bar");

  @BeforeClass
  public static void setup() throws Exception {

    assertTrue(foo.mkdir());
    assertTrue(bar.mkdir());

    System.setProperty(AccumuloVFSClassLoader.VFS_CLASSLOADER_DEBUG, "true");
    ContextConfig cc1 = new ContextConfig();
    cc1.setClassPath(foo.toURI() + ".*");
    cc1.setPostDelegate(true);
    cc1.setMonitorIntervalMs(1000);
    Context c1 = new Context();
    c1.setName("cx1");
    c1.setConfig(cc1);

    ContextConfig cc2 = new ContextConfig();
    cc2.setClassPath(bar.toURI() + ".*");
    cc2.setPostDelegate(false);
    cc2.setMonitorIntervalMs(1000);
    Context c2 = new Context();
    c2.setName("cx2");
    c2.setConfig(cc2);

    List<Context> list = new ArrayList<>();
    list.add(c1);
    list.add(c2);
    c.setContexts(list);
  }

  @Test
  public void testDeSer() throws Exception {
    Gson g = new Gson().newBuilder().setPrettyPrinting().create();
    String contexts = g.toJson(c);
    System.out.println(contexts);

    Gson g2 = new Gson();
    Contexts actual = g2.fromJson(contexts, Contexts.class);

    assertEquals(c, actual);

  }

  @Test
  public void testCreation() throws Exception {

    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        new File(foo, "HelloWorld.jar"));
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        new File(bar, "HelloWorld2.jar"));

    File f = TEMP.newFile();
    f.deleteOnExit();
    Gson g = new Gson();
    String contexts = g.toJson(c);
    try (BufferedWriter writer = Files.newBufferedWriter(f.toPath(), UTF_8, WRITE)) {
      writer.write(contexts);
    }
    ReloadingVFSContextClassLoaderFactory cl = new ReloadingVFSContextClassLoaderFactory() {
      @Override
      protected String getConfigFileLocation() {
        return f.toURI().toString();
      }
    };
    try {
      cl.getClassLoader("c1");
      fail("Expected illegal argument exception");
    } catch (IllegalArgumentException e) {
      // works
    }
    cl.getClassLoader("cx1");
    cl.getClassLoader("cx2");
    cl.closeForTests();
  }

  @Test
  public void testReloading() throws Exception {

    System.setProperty(AccumuloVFSClassLoader.VFS_CLASSPATH_MONITOR_INTERVAL, "1");

    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        new File(foo, "HelloWorld.jar"));
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        new File(bar, "HelloWorld2.jar"));

    File f = TEMP.newFile();
    f.deleteOnExit();
    Gson g = new Gson();
    String contexts = g.toJson(c);
    try (BufferedWriter writer = Files.newBufferedWriter(f.toPath(), UTF_8, WRITE)) {
      writer.write(contexts);
    }

    TestReloadingVFSContextClassLoaderFactory factory =
        new TestReloadingVFSContextClassLoaderFactory(foo.toURI() + ".*") {
          @Override
          protected String getConfigFileLocation() {
            return f.toURI().toString();
          }
        };

    ClassLoader cl1 = factory.getClassLoader("cx1");
    Class<?> clazz1 = cl1.loadClass("test.HelloWorld");
    Object o1 = clazz1.getDeclaredConstructor().newInstance();
    assertEquals("Hello World!", o1.toString());

    // Check that the class is the same before the update
    Class<?> clazz1_5 = cl1.loadClass("test.HelloWorld");
    assertEquals(clazz1, clazz1_5);

    assertTrue(new File(foo, "HelloWorld.jar").delete());

    Thread.sleep(1000);

    // Update the class
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        new File(foo, "HelloWorld2.jar"));

    // Wait for the monitor to notice
    Thread.sleep(1000);

    ClassLoader cl2 = factory.getClassLoader("cx1");
    assertNotEquals(cl1, cl2);

    Class<?> clazz2 = factory.getClassLoader("cx1").loadClass("test.HelloWorld");
    Object o2 = clazz2.getDeclaredConstructor().newInstance();
    assertEquals("Hello World!", o2.toString());

    // This is false because they are loaded by a different classloader
    assertNotEquals(clazz1, clazz2);
    assertNotEquals(o1, o2);

    factory.closeForTests();
  }

}
