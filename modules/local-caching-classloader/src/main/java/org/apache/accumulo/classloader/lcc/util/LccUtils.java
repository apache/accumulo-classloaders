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

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class LccUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LccUtils.class);

  public static final DigestUtils DIGESTER = new DigestUtils(DigestUtils.getSha256Digest());

  private static final Cleaner CLEANER = Cleaner.create();

  @SuppressFBWarnings(value = "DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED",
      justification = "doPrivileged is deprecated without replacement and removed in newer Java")
  public static URLClassLoader createClassLoader(String name, URL[] urls) {
    final var parent = LccUtils.class.getClassLoader();
    final var cl = new URLClassLoader(name, urls, parent);

    // a pass-through wrapper for the URLClassLoader, so the Cleaner can monitor for this proxy
    // object's reachability, but then the clean action can clean up the real URLClassLoader when
    // the proxy object becomes phantom-reachable (this won't work)
    var proxiedCl = (URLClassLoader) Proxy.newProxyInstance(parent,
        getInterfaces(URLClassLoader.class).toArray(new Class<?>[0]), (obj, method, args) -> {
          try {
            return method.invoke(cl, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });

    CLEANER.register(proxiedCl, () -> {
      try {
        cl.close();
      } catch (IOException e) {
        LOG.debug("Problem closing phantom-reachable classloader instance {}", name);
      }
    });

    if (LOG.isTraceEnabled()) {
      LOG.trace("New classloader created for {} from URLs: {}", name, Arrays.asList(urls));
    }
    return proxiedCl;
  }

  private static Set<Class<?>> getInterfaces(Class<?> clazz) {
    var set = new HashSet<Class<?>>();
    if (clazz != null) {
      set.addAll(getInterfaces(clazz.getSuperclass()));
      for (Class<?> interfaze : clazz.getInterfaces()) {
        set.add(interfaze);
      }
    }
    return set;
  }
}
