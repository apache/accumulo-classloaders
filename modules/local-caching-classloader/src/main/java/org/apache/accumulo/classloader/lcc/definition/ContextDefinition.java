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

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.classloader.lcc.Constants;
import org.apache.accumulo.classloader.lcc.resolvers.FileResolver;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;

import com.google.common.base.Preconditions;

public class ContextDefinition {

  public static ContextDefinition create(String contextName, int monitorIntervalSecs,
      URL... sources) throws ContextClassLoaderException, IOException {
    LinkedHashSet<Resource> resources = new LinkedHashSet<>();
    for (URL u : sources) {
      FileResolver resolver = FileResolver.resolve(u);
      try (InputStream is = resolver.getInputStream()) {
        String checksum = Constants.getChecksummer().digestAsHex(is);
        resources.add(new Resource(u, checksum));
      }
    }
    return new ContextDefinition(contextName, monitorIntervalSecs, resources);
  }

  private String contextName;
  private volatile int monitorIntervalSeconds;
  private LinkedHashSet<Resource> resources;
  private volatile transient String checksum = null;

  public ContextDefinition() {}

  public ContextDefinition(String contextName, int monitorIntervalSeconds,
      LinkedHashSet<Resource> resources) {
    this.contextName = requireNonNull(contextName, "context name must be supplied");
    Preconditions.checkArgument(monitorIntervalSeconds > 0,
        "monitor interval must be greater than zero");
    this.monitorIntervalSeconds = monitorIntervalSeconds;
    this.resources = requireNonNull(resources, "resources must be supplied");
  }

  public String getContextName() {
    return contextName;
  }

  public int getMonitorIntervalSeconds() {
    return monitorIntervalSeconds;
  }

  public Set<Resource> getResources() {
    return Collections.unmodifiableSet(resources);
  }

  @Override
  public int hashCode() {
    return hash(contextName, monitorIntervalSeconds, resources);
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
    return Objects.equals(contextName, other.contextName)
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
    return Constants.GSON.toJson(this);
  }
}
