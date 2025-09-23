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
package org.apache.accumulo.classloader.vfs.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.classloader.vfs.SimpleHDFSClassLoaderFactory;
import org.apache.accumulo.classloader.vfs.SimpleHDFSClassLoaderFactory.Context;
import org.apache.accumulo.classloader.vfs.SimpleHDFSClassLoaderFactory.JarInfo;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class SimpleHDFSClassLoaderFactoryTest {
  // TODO KEVIN RATHBUN SimpleHDFSClassLoaderFactory and this test need to be moved to their own
  // package (currently in vfs-class-loader)
  private static final Logger LOG = LoggerFactory.getLogger(SimpleHDFSClassLoaderFactoryTest.class);
  private static final String context1 = "context1";
  private static final Path contextsDir =
      Path.of(System.getProperty("user.dir"), "target", "contexts");
  private static final Path context1Dir = contextsDir.resolve(context1);
  private static final Path context1Manifest = context1Dir.resolve("manifest.json");

  @BeforeAll
  public static void setup() throws Exception {
    assertTrue(contextsDir.toFile().mkdir());
    assertTrue(context1Dir.toFile().mkdir());

    JarInfo jarInfo1 = new JarInfo("HelloWorld.jar", "123abc");
    Context context = new Context(context1, List.of(jarInfo1));

    Gson gson = new Gson().newBuilder().setPrettyPrinting().create();
    try (FileWriter writer = new FileWriter(context1Manifest.toFile(), StandardCharsets.UTF_8)) {
      gson.toJson(context, writer);
    }
  }

  @AfterAll
  public static void teardown() throws Exception {
    FileUtils.deleteDirectory(contextsDir.toFile());
  }

  @Test
  public void test() throws Exception {
    FileUtils.copyURLToFile(this.getClass().getResource("/HelloWorld.jar"),
        context1Dir.resolve("HelloWorld.jar").toFile());

    var classLoaderFactory = new SimpleHDFSClassLoaderFactory(null);
    var classLoader = classLoaderFactory.getClassLoader(context1);
    var clazz = classLoader.loadClass("test.HelloWorld");
    var methods = clazz.getMethods();
    assertEquals(1,
        Arrays.stream(methods).filter(method -> method.getName().equals("helloWorld")).count());
  }
}
