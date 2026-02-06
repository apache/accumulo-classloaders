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
import org.apache.accumulo.classloader.ccl.LocalStore;
import org.apache.accumulo.classloader.ccl.manifest.Manifest;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class InitializeCache implements KeywordExecutable {

  static class Opts extends Help {
    @Parameter(names = {"-d", "--directory"}, required = true,
        description = "the local directory to initialize and stage", arity = 1, order = 1)
    String directory;

    @Parameter(names = {"-v", "--verify"}, required = false,
        description = "also verify existing files if they were found already present", arity = 0,
        order = 2)
    boolean verify;

    @Parameter(required = false,
        description = "URLs for context manifests to load (<url>[ <url>...])", arity = -1,
        order = 3)
    public List<String> manifests = new ArrayList<>();
  }

  public InitializeCache() {}

  @Override
  public String keyword() {
    return "init-classloader-cache-dir";
  }

  @Override
  public String description() {
    return "Initializes the specified directory for the "
        + CachingClassLoaderFactory.class.getSimpleName()
        + " and stages any resources for the specified context manifest locations";
  }

  @Override
  public void execute(String[] args) throws Exception {
    var opts = new Opts();
    opts.parseArgs(InitializeCache.class.getName(), args);
    var localStore = new LocalStore(opts.directory, (a, b) -> {/* allow all */});
    for (String manifestUrl : opts.manifests) {
      var manifest = Manifest.download(new URL(manifestUrl));
      localStore.storeContext(manifest);
      if (opts.verify) {
        var workingDir = localStore.createWorkingHardLinks(manifest, p -> {/* do nothing */});
        LocalStore.recursiveDelete(workingDir);
      }
    }
  }
}
