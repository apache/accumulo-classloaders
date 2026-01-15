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
package org.apache.accumulo.classloader.lcc.definition;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.classloader.lcc.util.LccUtils.DIGESTER;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@AutoService(KeywordExecutable.class)
public class ContextDefinition implements KeywordExecutable {

  static class Opts extends Help {
    @Parameter(names = {"-i", "--interval"}, required = true,
        description = "monitor interval (in seconds)", arity = 1, order = 1)
    int monitorInterval;

    @Parameter(required = true, description = "classpath element URL (<url>[ <url>...])",
        arity = -1, order = 2)
    public List<String> files = new ArrayList<>();
  }

  private static final Gson GSON =
      new GsonBuilder().disableJdkUnsafe().setPrettyPrinting().create();

  public static ContextDefinition create(int monitorIntervalSecs, URL... sources)
      throws IOException {
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    for (URL u : sources) {
      FileResolver resolver = FileResolver.resolve(u);
      try (InputStream is = resolver.getInputStream()) {
        String checksum = DIGESTER.digestAsHex(is);
        resources.add(new Resource(u, checksum));
      }
    }
    return new ContextDefinition(monitorIntervalSecs, resources);
  }

  public static ContextDefinition fromRemoteURL(final URL url) throws IOException {
    final FileResolver resolver = FileResolver.resolve(url);
    try (InputStream is = resolver.getInputStream()) {
      var def = GSON.fromJson(new InputStreamReader(is, UTF_8), ContextDefinition.class);
      if (def == null) {
        throw new EOFException("InputStream does not contain a valid ContextDefinition at " + url);
      }
      return def;
    }
  }

  // transient fields that don't go in the json
  private final transient Supplier<String> checksum =
      Suppliers.memoize(() -> DIGESTER.digestAsHex(toJson()));

  // serialized fields for json
  // use a LinkedHashSet to preserve the order specified in the context file
  private int monitorIntervalSeconds;
  private LinkedHashSet<Resource> resources;

  public ContextDefinition() {}

  public ContextDefinition(int monitorIntervalSeconds, LinkedHashSet<Resource> resources) {
    Preconditions.checkArgument(monitorIntervalSeconds > 0,
        "monitor interval must be greater than zero");
    this.monitorIntervalSeconds = monitorIntervalSeconds;
    this.resources = requireNonNull(resources, "resources must be supplied");
  }

  public int getMonitorIntervalSeconds() {
    return monitorIntervalSeconds;
  }

  public Set<Resource> getResources() {
    return Collections.unmodifiableSet(resources);
  }

  @Override
  public int hashCode() {
    return hash(monitorIntervalSeconds, resources);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ContextDefinition other = (ContextDefinition) obj;
    return monitorIntervalSeconds == other.monitorIntervalSeconds
        && Objects.equals(resources, other.resources);
  }

  public String getChecksum() {
    return checksum.get();
  }

  public String toJson() {
    return GSON.toJson(this);
  }

  @Override
  public String keyword() {
    return "create-context-definition";
  }

  @Override
  public String description() {
    return "Creates and prints a Context Definition";
  }

  @Override
  public void execute(String[] args) throws Exception {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(new Configuration()));

    Opts opts = new Opts();
    opts.parseArgs(ContextDefinition.class.getName(), args);
    URL[] urls = new URL[opts.files.size()];
    int count = 0;
    for (String f : opts.files) {
      urls[count++] = new URL(f);
    }
    ContextDefinition def = create(opts.monitorInterval, urls);
    System.out.println(def.toJson());
  }
}
