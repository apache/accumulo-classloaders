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
package org.apache.accumulo.classloader.lcc;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.net.URLClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalCachingContextCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(LocalCachingContextCleaner.class);
  private static final Cleaner CLEANER = Cleaner.create();

  public static void registerClassLoader(final URLClassLoader cl) {
    CLEANER.register(cl, () -> {
      try {
        cl.close();
      } catch (IOException ioe) {
        LOG.warn("Error closing LocalCachingContext URLClassLoader", ioe);
      }
    });
  }

}
