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
package org.apache.accumulo.classloader.ccl.manifest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class Manifest {

  // pretty-print uses Unix newline
  private static final Gson GSON =
      new GsonBuilder().disableJdkUnsafe().setPrettyPrinting().create();

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "user-supplied URL is the intended functionality")
  public static Manifest create(int monitorIntervalSecs, String algorithm, URL... sources)
      throws IOException {
    // use a LinkedHashSet to preserve the order of the resources
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    for (URL u : sources) {
      try (InputStream is = new BufferedInputStream(u.openStream())) {
        String checksum = new DigestUtils(algorithm).digestAsHex(is);
        resources.add(new Resource(u, algorithm, checksum));
      }
    }
    return new Manifest(monitorIntervalSecs, resources);
  }

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "user-supplied URL is the intended functionality")
  public static Manifest download(final URL url) throws IOException {
    try (InputStream is = url.openStream()) {
      var manifest = GSON.fromJson(new InputStreamReader(is, UTF_8), Manifest.class);
      if (manifest == null) {
        throw new EOFException("InputStream does not contain a valid manifest at " + url);
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

  public Manifest() {}

  public Manifest(int monitorIntervalSeconds, LinkedHashSet<Resource> resources) {
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
    Manifest other = (Manifest) obj;
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

}
