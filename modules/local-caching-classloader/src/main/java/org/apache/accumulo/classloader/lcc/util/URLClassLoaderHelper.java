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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A simple utility class to hold the parameters necessary for constructing a URLClassLoader and a
 * convenience method to do the construction. This exists to hold the URLClassLoader name and URLs
 * in a single object, so it can be provided by a single Supplier.
 */
public class URLClassLoaderHelper {

  private static final Logger LOG = LoggerFactory.getLogger(URLClassLoaderHelper.class);

  private final String name;
  private final URL[] urls;

  public URLClassLoaderHelper(final String name, final URL[] urls) {
    this.name = name;
    this.urls = Arrays.copyOf(urls, urls.length);
  }

  @SuppressFBWarnings(value = "DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED",
      justification = "doPrivileged is deprecated without replacement and removed in newer Java")
  public URLClassLoader createClassLoader() {
    final var cl = new URLClassLoader(name, urls, getClass().getClassLoader());
    if (LOG.isTraceEnabled()) {
      LOG.trace("New classloader created from URLs: {}", Arrays.asList(urls));
    }
    return cl;
  }

}
