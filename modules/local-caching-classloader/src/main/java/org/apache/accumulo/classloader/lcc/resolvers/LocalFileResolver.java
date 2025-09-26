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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils;

public class LocalFileResolver extends FileResolver {

  private final File file;

  public LocalFileResolver(URL url) throws ContextClassLoaderException {
    super(url);
    if (url.getHost() != null) {
      throw new ContextClassLoaderException(
          "Unsupported file url, only local files are supported. url = " + url);
    }
    try {
      final URI uri = url.toURI();
      final Path path = Paths.get(uri);
      if (Files.notExists(Paths.get(uri))) {
        throw new ContextClassLoaderException("File: " + url + " does not exist.");
      }
      file = path.toFile();
    } catch (URISyntaxException e) {
      throw new ContextClassLoaderException("Error creating URI from url: " + url, e);
    }
  }

  @Override
  public FileInputStream getInputStream() throws ContextClassLoaderException {
    try {
      return FileUtils.openInputStream(file);
    } catch (IOException e) {
      throw new ContextClassLoaderException("Error opening file at url: " + url, e);
    }
  }
}
