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

import org.apache.accumulo.classloader.lcc.LocalCachingContextClassLoaderFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class LccUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LccUtils.class);

  public static final DigestUtils DIGESTER = new DigestUtils(DigestUtils.getSha256Digest());

  @SuppressFBWarnings(value = "DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED",
      justification = "doPrivileged is deprecated without replacement and removed in newer Java")
  public static URLClassLoader createClassLoader(String name, URL[] urls) {
    final var cl = new URLClassLoader(name, urls,
        LocalCachingContextClassLoaderFactory.class.getClassLoader());
    LOG.info("New classloader created for {}", name);
    return cl;
  }
}
