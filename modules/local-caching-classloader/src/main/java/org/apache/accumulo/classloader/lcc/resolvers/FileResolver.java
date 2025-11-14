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
package org.apache.accumulo.classloader.lcc.resolvers;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;

public abstract class FileResolver {

  public static FileResolver resolve(URL url) throws ContextClassLoaderException {
    String protocol = url.getProtocol();
    switch (protocol) {
      case "http":
      case "https":
        return new HttpFileResolver(url);
      case "file":
        return new LocalFileResolver(url);
      case "hdfs":
        return new HdfsFileResolver(url);
      default:
        throw new ContextClassLoaderException("Unhandled protocol: " + protocol);
    }
  }

  protected final URL url;

  protected FileResolver(URL url) throws ContextClassLoaderException {
    this.url = url;
  }

  public URL getURL() {
    return this.url;
  }

  public abstract String getFileName() throws URISyntaxException;

  public abstract InputStream getInputStream() throws ContextClassLoaderException;

}
