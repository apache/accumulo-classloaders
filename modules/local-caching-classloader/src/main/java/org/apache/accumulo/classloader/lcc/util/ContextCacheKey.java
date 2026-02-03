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
package org.apache.accumulo.classloader.lcc.util;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.accumulo.classloader.lcc.definition.ContextDefinition;

public class ContextCacheKey {
  private final String location;
  private final ContextDefinition definition;

  public ContextCacheKey(String location, ContextDefinition definition) {
    this.location = requireNonNull(location);
    this.definition = requireNonNull(definition);
  }

  public String getLocation() {
    return location;
  }

  public ContextDefinition getContextDefinition() {
    return definition;
  }

  @Override
  public int hashCode() {
    return hash(location, definition);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ContextCacheKey) {
      var o = (ContextCacheKey) obj;
      return Objects.equals(location, o.location) && Objects.equals(definition, o.definition);
    }
    return false;
  }

  @Override
  public String toString() {
    return definition.getChecksumAlgorithm() + " (" + location + ") = " + definition.getChecksum();
  }

}
