/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.classloader.vfs.context;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.accumulo.classloader.vfs.ReloadingVFSClassLoader;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;

import com.google.gson.Gson;

/**
 * A ClassLoaderFactory implementation that uses a ReloadingVFSClassLoader per defined context.
 * Configuration of this class is done with a JSON file whose location is defined by the system
 * property <b>vfs.context.class.loader.config</b>. To use this ClassLoaderFactory you need to set
 * the Accumulo configuration property <b>general.context.class.loader.factory</b> to the fully
 * qualified name of this class, create a configuration file that defines the supported contexts and
 * their configuration, and set the system property <b>vfs.context.class.loader.config</b> to the
 * location of the configuration file.
 *
 * <p>
 * Example configuration file:
 *
 * <pre>
 * {
 *  "contexts": [
 *    {
 *      "name": "cx1",
 *      "config": {
 *        "classPath": "file:///tmp/foo",
 *        "postDelegate": true,
 *        "monitorIntervalMs": 30000
 *      }
 *    },
 *    {
 *      "name": "cx2",
 *      "config": {
 *        "classPath": "file:///tmp/bar",
 *        "postDelegate": false,
 *        "monitorIntervalMs": 30000
 *      }
 *    }
 *  ]
 * }
 * </pre>
 */
public class ReloadingVFSContextClassLoaderFactory implements ContextClassLoaderFactory {

  public static class Contexts {
    List<Context> contexts;

    public List<Context> getContexts() {
      return contexts;
    }

    public void setContexts(List<Context> contexts) {
      this.contexts = contexts;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((contexts == null) ? 0 : contexts.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Contexts other = (Contexts) obj;
      if (contexts == null) {
        if (other.contexts != null)
          return false;
      } else if (!contexts.equals(other.contexts))
        return false;
      return true;
    }
  }

  public static class Context {
    private String name;
    private ContextConfig config;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public ContextConfig getConfig() {
      return config;
    }

    public void setConfig(ContextConfig config) {
      this.config = config;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((config == null) ? 0 : config.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Context other = (Context) obj;
      if (config == null) {
        if (other.config != null)
          return false;
      } else if (!config.equals(other.config))
        return false;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      return true;
    }
  }

  public static class ContextConfig {
    private String classPath;
    private boolean postDelegate;
    private long monitorIntervalMs;

    public String getClassPath() {
      return classPath;
    }

    public void setClassPath(String classPath) {
      this.classPath = ReloadingVFSClassLoader.replaceEnvVars(classPath, System.getenv());
    }

    public boolean getPostDelegate() {
      return postDelegate;
    }

    public void setPostDelegate(boolean postDelegate) {
      this.postDelegate = postDelegate;
    }

    public long getMonitorIntervalMs() {
      return monitorIntervalMs;
    }

    public void setMonitorIntervalMs(long monitorIntervalMs) {
      this.monitorIntervalMs = monitorIntervalMs;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((classPath == null) ? 0 : classPath.hashCode());
      result = prime * result + (int) (monitorIntervalMs ^ (monitorIntervalMs >>> 32));
      result = prime * result + (postDelegate ? 1231 : 1237);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ContextConfig other = (ContextConfig) obj;
      if (classPath == null) {
        if (other.classPath != null)
          return false;
      } else if (!classPath.equals(other.classPath))
        return false;
      if (monitorIntervalMs != other.monitorIntervalMs)
        return false;
      if (postDelegate != other.postDelegate)
        return false;
      return true;
    }
  }

  public static final String CONFIG_LOCATION = "vfs.context.class.loader.config";
  private static final Map<String,ReloadingVFSClassLoader> CONTEXTS = new HashMap<>();

  protected String getConfigFileLocation() {
    String loc = System.getProperty(CONFIG_LOCATION);
    if (null == loc || loc.isBlank()) {
      throw new RuntimeException(CONFIG_LOCATION
          + " system property must be set to use ReloadingVFSContextClassLoaderFactory");
    }
    return loc;
  }

  @Override
  public void initialize(Supplier<Map<String,String>> contextProperties) throws Exception {
    // Properties
    File f = new File(getConfigFileLocation());
    if (!f.canRead()) {
      throw new RuntimeException("Unable to read configuration file: " + f.getAbsolutePath());
    }
    Gson g = new Gson();
    Contexts con = g.fromJson(Files.newBufferedReader(f.toPath()), Contexts.class);

    con.getContexts().forEach(c -> {
      CONTEXTS.put(c.getName(), new ReloadingVFSClassLoader(
          ReloadingVFSContextClassLoaderFactory.class.getClassLoader()) {
        @Override
        protected String getClassPath() {
          return c.getConfig().getClassPath();
        }

        @Override
        protected boolean isPreDelegationModel() {
          return !(c.getConfig().getPostDelegate());
        }

        @Override
        protected long getMonitorInterval() {
          return c.getConfig().getMonitorIntervalMs();
        }

        @Override
        protected boolean isVMInitialized() {
          // The classloader is not being set using
          // `java.system.class.loader`, so the VM is initialized.
          return true;
        }
      });
    });
  }

  @Override
  public ClassLoader getClassLoader(String contextName) throws IllegalArgumentException {
    if (!CONTEXTS.containsKey(contextName)) {
      throw new IllegalArgumentException(
          "ReloadingVFSContextClassLoaderFactory not configured for context: " + contextName);
    }
    return CONTEXTS.get(contextName);
  }

}
