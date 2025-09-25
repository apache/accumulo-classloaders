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
package org.apache.accumulo.classloaders.lcc.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.classloader.lcc.Constants;
import org.apache.accumulo.classloader.lcc.manifest.ContextDefinition;
import org.apache.accumulo.classloader.lcc.manifest.Manifest;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory.ContextClassLoaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Contexts {

  private static final Logger LOG = LoggerFactory.getLogger(Contexts.class);

  private final AtomicReference<Manifest> manifest;
  private final Map<String,ContextClassLoader> contexts = new HashMap<>();
  private final AtomicBoolean updating = new AtomicBoolean(true);

  public Contexts(AtomicReference<Manifest> manifest) {
    this.manifest = manifest;
  }

  public synchronized void update() {
    LOG.debug("Updating all contexts using Manifest");
    updating.set(true);
    final Map<String,ContextDefinition> ctx = manifest.get().getContexts();
    final List<String> removals = new ArrayList<>();
    contexts.keySet().forEach(k -> {
      if (!ctx.containsKey(k)) {
        removals.add(k);
      }
    });

    removals.forEach(r -> {
      LOG.debug("Context {} is no longer contained in the Manifest, removing", r);
      contexts.get(r).clear();
      contexts.remove(r);
    });

    final List<Future<?>> futures = new ArrayList<>();
    for (Entry<String,ContextDefinition> e : ctx.entrySet()) {
      if (contexts.get(e.getKey()) == null) {
        // This is a newly defined context
        LOG.debug("Context {} is new in the Manifest, creating new ContextClassLoader", e.getKey());
        try {
          ContextClassLoader ccl = new ContextClassLoader(manifest, e.getKey());
          contexts.put(e.getKey(), ccl);
          futures.add(Constants.EXECUTOR.submit(() -> ccl.initialize()));
        } catch (ContextClassLoaderException e1) {
          LOG.error("Error creating new ContextClassLoader for context: " + e.getKey(), e1);
        }
      }
    }

    while (!futures.isEmpty()) {
      Iterator<Future<?>> iter = futures.iterator();
      while (iter.hasNext()) {
        Future<?> f = iter.next();
        if (f.isDone()) {
          iter.remove();
          try {
            f.get();
          } catch (InterruptedException | ExecutionException | CancellationException ex) {
            LOG.warn("Updating ContextClassLoader with ContextDefinition change failed.", ex);
          }
        }
      }
    }

    updating.set(false);
  }

  public ClassLoader getContextClassLoader(String context) {
    return contexts.get(context).getClassloader();
  }

}
