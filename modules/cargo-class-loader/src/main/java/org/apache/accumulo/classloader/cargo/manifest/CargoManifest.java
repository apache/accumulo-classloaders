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
package org.apache.accumulo.classloader.cargo.manifest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.io.BufferedInputStream;
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
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.codec.digest.DigestUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
public class CargoManifest implements KeywordExecutable {

  static class Opts {
    @Parameter(names = {"-i", "--interval"}, required = true,
        description = "monitor interval (in seconds)", arity = 1, order = 1)
    int monitorInterval;

    @Parameter(names = {"-a", "--algorithm"}, required = false,
        description = "checksum algorithm to use (default: " + SHA_512 + ")", arity = 1, order = 2)
    String algorithm = SHA_512;

    @Parameter(required = true, description = "classpath element URL (<url>[ <url>...])",
        arity = -1, order = 3)
    public List<String> files = new ArrayList<>();

    @Parameter(names = {"-h", "-?", "--help", "-help"}, help = true)
    public boolean help = false;

    void parseArgs(Consumer<JCommander> jcConsumer, String programName, String[] args,
        Object... others) {
      JCommander commander = new JCommander();
      jcConsumer.accept(commander);
      commander.addObject(this);
      for (Object other : others) {
        commander.addObject(other);
      }
      commander.setProgramName(programName);
      try {
        commander.parse(args);
      } catch (ParameterException ex) {
        commander.usage();
        exitWithError(ex.getMessage(), 1);
      }
      if (help) {
        commander.usage();
        exit(0);
      }
    }

    void parseArgs(String programName, String[] args, Object... others) {
      parseArgs(jCommander -> {}, programName, args, others);
    }

    void exit(int status) {
      System.exit(status);
    }

    void exitWithError(String message, int status) {
      System.err.println(message);
      exit(status);
    }
  }

  // pretty-print uses Unix newline
  private static final Gson GSON =
      new GsonBuilder().disableJdkUnsafe().setPrettyPrinting().create();

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "user-supplied URL is the intended functionality")
  public static CargoManifest create(int monitorIntervalSecs, String algorithm, URL... sources)
      throws IOException {
    // use a LinkedHashSet to preserve the order of the cargo resources
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    for (URL u : sources) {
      try (InputStream is = new BufferedInputStream(u.openStream())) {
        String checksum = new DigestUtils(algorithm).digestAsHex(is);
        resources.add(new Resource(u, algorithm, checksum));
      }
    }
    return new CargoManifest(monitorIntervalSecs, resources);
  }

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "user-supplied URL is the intended functionality")
  public static CargoManifest download(final URL url) throws IOException {
    try (InputStream is = url.openStream()) {
      var manifest = GSON.fromJson(new InputStreamReader(is, UTF_8), CargoManifest.class);
      if (manifest == null) {
        throw new EOFException("InputStream does not contain a valid CargoManifest at " + url);
      }
      return manifest;
    }
  }

  private static final String SHA_512 = "SHA-512";

  // transient fields that don't go in the json
  private final transient Supplier<String> checksum =
      Suppliers.memoize(() -> new DigestUtils(getChecksumAlgorithm()).digestAsHex(toJson()));

  // serialized fields for json
  // use a LinkedHashSet to preserve the order specified in the file
  private int monitorIntervalSeconds;
  private LinkedHashSet<Resource> resources;

  public CargoManifest() {}

  public CargoManifest(int monitorIntervalSeconds, LinkedHashSet<Resource> resources) {
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
    CargoManifest other = (CargoManifest) obj;
    return monitorIntervalSeconds == other.monitorIntervalSeconds
        && Objects.equals(resources, other.resources);
  }

  public String getChecksumAlgorithm() {
    return SHA_512;
  }

  public String getChecksum() {
    return checksum.get();
  }

  public String toJson() {
    // GSON pretty print uses Unix line-endings, and may or may not have a trailing one, so
    // ensure a trailing one exists, so it's included in checksum computations and in
    // any files written from this (for better readability)
    return GSON.toJson(this).stripTrailing() + "\n";
  }

  @Override
  public String keyword() {
    return "create-cargo-manifest";
  }

  @Override
  public String description() {
    return "Creates and prints a cargo manifest";
  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(CargoManifest.class.getName(), args);
    URL[] urls = new URL[opts.files.size()];
    int count = 0;
    for (String f : opts.files) {
      urls[count++] = new URL(f);
    }
    System.out.print(create(opts.monitorInterval, opts.algorithm, urls).toJson());
  }
}
