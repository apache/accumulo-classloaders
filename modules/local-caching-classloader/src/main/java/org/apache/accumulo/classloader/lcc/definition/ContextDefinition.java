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

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;

import org.apache.accumulo.classloader.lcc.Constants;

public class ContextDefinition {
  private final String contextName;
  private final int monitorIntervalSeconds;
  private final List<Resource> resources;
  private volatile transient byte[] checksum = null;

  public ContextDefinition(String contextName, int monitorIntervalSeconds,
      List<Resource> resources) {
    this.contextName = contextName;
    this.monitorIntervalSeconds = monitorIntervalSeconds;
    this.resources = resources;
  }

  public String getContextName() {
    return contextName;
  }

  public int getMonitorIntervalSeconds() {
    return monitorIntervalSeconds;
  }

  public List<Resource> getResources() {
    return resources;
  }

  @Override
  public int hashCode() {
    return Objects.hash(contextName, monitorIntervalSeconds, resources);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContextDefinition other = (ContextDefinition) obj;
    return Objects.equals(contextName, other.contextName)
        && monitorIntervalSeconds == other.monitorIntervalSeconds
        && Objects.equals(resources, other.resources);
  }

  public synchronized byte[] getChecksum() throws NoSuchAlgorithmException {
    if (checksum == null) {
      checksum = Constants.getChecksummer().digest(Constants.GSON.toJson(this));
    }
    return checksum;
  }

}
