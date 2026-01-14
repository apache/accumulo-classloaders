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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;

/**
 * A simple de-duplication cache of weakly referenced values that retains a strong reference for a
 * minimum amount of time specified, to prevent garbage collection too frequently for objects that
 * may be used again.
 */
public class DeduplicationCache<KEY,PARAMS,VALUE> {

  private static final Logger LOG = LoggerFactory.getLogger(DeduplicationCache.class);

  private final Cache<KEY,VALUE> canonicalWeakValuesCache;
  private final Cache<KEY,VALUE> expireAfterAccessStrongRefs;
  private final BiFunction<KEY,PARAMS,VALUE> loaderFunction;

  private final RemovalListener<KEY,VALUE> logListener = (key, value, cause) -> LOG
      .trace("Entry removed due to {}. K = {}, V = {}", cause, key, value);

  public DeduplicationCache(final BiFunction<KEY,PARAMS,VALUE> loaderFunction,
      final Duration minLifetime, final RemovalListener<KEY,VALUE> listener) {
    this.loaderFunction = requireNonNull(loaderFunction);
    RemovalListener<KEY,VALUE> actualListener =
        listener == null ? logListener : (key, value, cause) -> {
          logListener.onRemoval(key, value, cause);
          listener.onRemoval(key, value, cause);
        };
    this.canonicalWeakValuesCache = Caffeine.newBuilder().weakValues()
        .evictionListener(actualListener).scheduler(Scheduler.systemScheduler()).build();
    this.expireAfterAccessStrongRefs =
        Caffeine.newBuilder().expireAfterAccess(requireNonNull(minLifetime))
            .evictionListener(actualListener).scheduler(Scheduler.systemScheduler()).build();
  }

  public VALUE computeIfAbsent(final KEY key, final Supplier<PARAMS> params) {
    var cl = canonicalWeakValuesCache.get(key, k -> loaderFunction.apply(k, params.get()));
    expireAfterAccessStrongRefs.put(key, cl);
    return cl;
  }

  public boolean anyMatch(final Predicate<KEY> keyPredicate) {
    canonicalWeakValuesCache.cleanUp();
    return canonicalWeakValuesCache.asMap().keySet().stream().anyMatch(keyPredicate);
  }

}
