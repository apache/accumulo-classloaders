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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

public class Resource implements Comparable<Resource> {

  private String location;
  private String checksum;

  public Resource() {}

  public Resource(String location, String checksum) {
    this.location = location;
    this.checksum = checksum;
  }

  public String getLocation() {
    return location;
  }

  public String getChecksum() {
    return checksum;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  public URL getURL() throws MalformedURLException {
    return new URL(location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(checksum, location);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Resource other = (Resource) obj;
    return Objects.equals(checksum, other.checksum) && Objects.equals(location, other.location);
  }

  @Override
  public int compareTo(Resource other) {
    int result = this.location.compareTo(other.location);
    if (result == 0) {
      return this.checksum.compareTo(other.checksum);
    }
    return result;
  }
}
