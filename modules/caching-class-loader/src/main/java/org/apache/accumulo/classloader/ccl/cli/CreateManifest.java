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
package org.apache.accumulo.classloader.ccl.cli;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.classloader.ccl.CachingClassLoaderFactory;
import org.apache.accumulo.classloader.ccl.manifest.Manifest;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class CreateManifest implements KeywordExecutable {

  static class Opts extends Help {
    @Parameter(names = {"-i", "--interval"}, required = true,
        description = "monitor interval (in seconds)", arity = 1, order = 1)
    int monitorInterval;

    @Parameter(names = {"-a", "--algorithm"}, required = false,
        description = "checksum algorithm to use (default: SHA-512)", arity = 1, order = 2)
    String algorithm = "SHA-512";

    @Parameter(required = true, description = "classpath element URL (<url>[ <url>...])",
        arity = -1, order = 3)
    public List<String> files = new ArrayList<>();
  }

  public CreateManifest() {}

  @Override
  public String keyword() {
    return "create-classloader-manifest";
  }

  @Override
  public String description() {
    return "Creates and prints a class loader context manifest for the "
        + CachingClassLoaderFactory.class.getSimpleName();
  }

  @Override
  public void execute(String[] args) throws Exception {
    var opts = new Opts();
    opts.parseArgs(CreateManifest.class.getName(), args);
    URL[] urls = new URL[opts.files.size()];
    int count = 0;
    for (String f : opts.files) {
      urls[count++] = new URL(f);
    }
    System.out.print(Manifest.create(opts.monitorInterval, opts.algorithm, urls).toJson());
  }
}
