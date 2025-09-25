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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileResolver extends FileResolver {

  private final Configuration hadoopConf = new Configuration();
  private final FileSystem fs;
  private final Path path;

  protected HdfsFileResolver(URL url) throws ContextClassLoaderException {
    super(url);
    try {
      final URI uri = url.toURI();
      this.fs = FileSystem.get(uri, hadoopConf);
      this.path = fs.makeQualified(new Path(uri));
      if (!fs.exists(this.path)) {
        throw new ContextClassLoaderException("File: " + url + " does not exist.");
      }
    } catch (URISyntaxException e) {
      throw new ContextClassLoaderException("Error creating URI from url: " + url, e);
    } catch (IOException e) {
      throw new ContextClassLoaderException("Error resolving file from url: " + url, e);
    }
  }

  @Override
  public InputStream getInputStream() throws ContextClassLoaderException {
    try {
      return fs.open(path);
    } catch (IOException e) {
      throw new ContextClassLoaderException("Error opening file at url: " + url, e);
    }
  }
}
