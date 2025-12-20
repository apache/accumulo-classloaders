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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.classloader.lcc.Constants;
import org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ContextDefinition {

  private static final Gson GSON =
      new GsonBuilder().disableJdkUnsafe().setPrettyPrinting().create();

  // for testing
  public static ContextDefinition create(String sourceFileName, int monitorIntervalSecs,
      URL... sources) throws IOException {
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    for (URL u : sources) {
      FileResolver resolver = FileResolver.resolve(u);
      try (InputStream is = resolver.getInputStream()) {
        String checksum = Constants.getChecksummer().digestAsHex(is);
        resources.add(new Resource(u, checksum));
      }
    }
    return new ContextDefinition(sourceFileName, monitorIntervalSecs, resources);
  }

  public static ContextDefinition fromRemoteURL(final URL url) throws IOException {
    LocalCachingContextClassLoaderFactory.LOG.trace("Retrieving context definition file from {}",
        url);
    final FileResolver resolver = FileResolver.resolve(url);
    try (InputStream is = resolver.getInputStream()) {
      var def = GSON.fromJson(new InputStreamReader(is, UTF_8), ContextDefinition.class);
      if (def == null) {
        throw new EOFException("InputStream does not contain a valid ContextDefinition at " + url);
      }
      def.sourceFileName = resolver.getFileName();
      def.localFileName = sourceToLocalFileName(def.sourceFileName);
      return def;
    }
  }

  // transient fields that don't go in the json
  private transient String sourceFileName;
  private transient String localFileName;
  private volatile transient String checksum = null;

  // serialized fields for json
  // use a LinkedHashSet to preserve the order specified in the context file
  private int monitorIntervalSeconds;
  private LinkedHashSet<Resource> resources;

  private static String sourceToLocalFileName(String sourceFileName) {
    return sourceFileName.toLowerCase().endsWith(".json") ? sourceFileName
        : sourceFileName + ".json";
  }

  public ContextDefinition() {}

  public ContextDefinition(String sourceFileName, int monitorIntervalSeconds,
      LinkedHashSet<Resource> resources) {
    this.sourceFileName = requireNonNull(sourceFileName, "source file name must be supplied");
    this.localFileName = sourceToLocalFileName(sourceFileName);

    Preconditions.checkArgument(monitorIntervalSeconds > 0,
        "monitor interval must be greater than zero");
    this.monitorIntervalSeconds = monitorIntervalSeconds;
    this.resources = requireNonNull(resources, "resources must be supplied");
  }

  public String getSourceFileName() {
    return sourceFileName;
  }

  public String getLocalFileName() {
    return localFileName;
  }

  public int getMonitorIntervalSeconds() {
    return monitorIntervalSeconds;
  }

  public Set<Resource> getResources() {
    return Collections.unmodifiableSet(resources);
  }

  @Override
  public int hashCode() {
    return hash(sourceFileName, monitorIntervalSeconds, resources);
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
    return Objects.equals(sourceFileName, other.sourceFileName)
        && monitorIntervalSeconds == other.monitorIntervalSeconds
        && Objects.equals(resources, other.resources);
  }

  public synchronized String getChecksum() {
    if (checksum == null) {
      checksum = Constants.getChecksummer().digestAsHex(toJson());
    }
    return checksum;
  }

  public String toJson() {
    return GSON.toJson(this);
  }
}
