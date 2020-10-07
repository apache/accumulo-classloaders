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
package org.apache.accumulo.classloader.vfs;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileMonitor;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * <p>
 * A {@code ClassLoader} implementation that watches for changes in any of the files/directories in
 * the classpath. When a change is noticed, this classloader will then load the new classes in
 * subsequent calls to loadClass. This classloader supports both the normal classloader
 * pre-delegation model and a post-delegation model. To enable the post-delegation feature set the
 * system property <b>vfs.class.loader.delegation</b> to "post".
 *
 * <p>
 * This classloader uses the following system properties:
 *
 * <ol>
 * <li><b>vfs.cache.dir</b> - for specifying the directory to use for the local VFS cache (default
 * is the system property <b>java.io.tmpdir</b></li>
 * <li><b>vfs.classpath.monitor.seconds</b> - for specifying the file system monitor (default:
 * 5m)</li>
 * <li><b>vfs.class.loader.classpath</b> - for specifying the class path</li>
 * <li><b>vfs.class.loader.delegation</b> - valid values are "pre" and "post" (default: pre)</li>
 * </ol>
 *
 * <p>
 * This class will attempt to perform substitution on any environment variables found in the values.
 * For example, the system property <b>vfs.cache.dir</b> can be set to <b>$HOME/cache</b>.
 */
public class ReloadingVFSClassLoader extends ClassLoader implements Closeable, FileListener {

  public static final String VFS_CLASSPATH_MONITOR_INTERVAL = "vfs.classpath.monitor.seconds";
  public static final String VFS_CACHE_DIR_PROPERTY = "vfs.cache.dir";
  public static final String VFS_CLASSLOADER_CLASSPATH = "vfs.class.loader.classpath";
  public static final String VFS_CLASSLOADER_DELEGATION = "vfs.class.loader.delegation";
  public static final String VFS_CLASSLOADER_DEBUG = "vfs.class.loader.debug";

  private static final String VFS_CACHE_DIR_DEFAULT = "java.io.tmpdir";

  // set to 5 mins. The rationale behind this large time is to avoid a gazillion tservers all asking
  // the name node for info too frequently.
  private static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

  private static boolean DEBUG = false;
  private static String CLASSPATH = null;
  private static Boolean PRE_DELEGATION = null;
  private static Long MONITOR_INTERVAL = null;
  private static boolean VM_INITIALIZED = false;

  private volatile long maxWaitInterval = 60000;
  private volatile long maxRetries = -1;
  private volatile long sleepInterval = 1000;
  private volatile boolean vfsInitializing = false;

  private final ClassLoader parent;
  private final ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock(true);
  private final String name;
  private final String classpath;
  private final Boolean preDelegation;
  private final long monitorInterval;
  private Optional<Updater> updater = Optional.empty();
  private FileObject[] files;
  private VFSClassLoaderWrapper cl = null;
  private DefaultFileSystemManager vfs = null;

  static {
    DEBUG = Boolean.parseBoolean(System.getProperty(VFS_CLASSLOADER_DEBUG, "false"));
    CLASSPATH = getClassPathProperty();
    PRE_DELEGATION = getPreDelegationModelProperty();
    MONITOR_INTERVAL = getMonitorIntervalProperty();
  }

  private static void printDebug(String msg) {
    if (!DEBUG)
      return;
    System.out
        .println(String.format("%d ReloadingVFSClassLoader: %s", System.currentTimeMillis(), msg));
  }

  private static void printError(String msg) {
    System.err
        .println(String.format("%d ReloadingVFSClassLoader: %s", System.currentTimeMillis(), msg));
  }

  /**
   * Get the classpath value from the environment and resolve embedded env vars
   *
   * @return classpath value
   */
  private static String getClassPathProperty() {
    String cp = System.getProperty(VFS_CLASSLOADER_CLASSPATH);
    if (null == cp || cp.isBlank()) {
      printError(VFS_CLASSLOADER_CLASSPATH + " system property not set, using default of \"\"");
      cp = "";
    }
    String result = replaceEnvVars(cp, System.getenv());
    printDebug("Classpath set to: " + result);
    return result;
  }

  /**
   * Get the delegation model
   *
   * @return true if pre delegaion, false if post delegation
   */
  private static boolean getPreDelegationModelProperty() {
    String delegation = System.getProperty(VFS_CLASSLOADER_DELEGATION);
    boolean preDelegation = true;
    if (null != delegation && delegation.equalsIgnoreCase("post")) {
      preDelegation = false;
    }
    printDebug("ClassLoader configured for pre-delegation: " + preDelegation);
    return preDelegation;
  }

  /**
   * Get the directory for the VFS cache
   *
   * @return VFS cache directory
   */
  static String getVFSCacheDir() {
    // Get configuration properties from the environment variables
    String vfsCacheDir = System.getProperty(VFS_CACHE_DIR_PROPERTY);
    if (null == vfsCacheDir || vfsCacheDir.isBlank()) {
      printError(VFS_CACHE_DIR_PROPERTY + " system property not set, using default of "
          + VFS_CACHE_DIR_DEFAULT);
      vfsCacheDir = System.getProperty(VFS_CACHE_DIR_DEFAULT);
    }
    String cache = replaceEnvVars(vfsCacheDir, System.getenv());
    printDebug("VFS Cache Dir set to: " + cache);
    return cache;
  }

  /**
   * Replace environment variables in the string with their actual value
   */
  public static String replaceEnvVars(String classpath, Map<String,String> env) {
    Pattern envPat = Pattern.compile("\\$[A-Za-z][a-zA-Z0-9_]*");
    Matcher envMatcher = envPat.matcher(classpath);
    while (envMatcher.find(0)) {
      // name comes after the '$'
      String varName = envMatcher.group().substring(1);
      String varValue = env.get(varName);
      if (varValue == null) {
        varValue = "";
      }
      classpath = (classpath.substring(0, envMatcher.start()) + varValue
          + classpath.substring(envMatcher.end()));
      envMatcher.reset(classpath);
    }
    return classpath;
  }

  /**
   * Get the file system monitor interval
   *
   * @return monitor interval in ms
   */
  private static long getMonitorIntervalProperty() {
    String interval = System.getProperty(VFS_CLASSPATH_MONITOR_INTERVAL);
    if (null != interval && !interval.isBlank()) {
      try {
        return TimeUnit.SECONDS.toMillis(Long.parseLong(interval));
      } catch (NumberFormatException e) {
        printError(VFS_CLASSPATH_MONITOR_INTERVAL + " system property not set, using default of "
            + DEFAULT_TIMEOUT);
        return DEFAULT_TIMEOUT;
      }
    }
    return DEFAULT_TIMEOUT;
  }

  private class Updater {

    /**
     * This task replaces the delegate classloader with a new instance when the filesystem has
     * changed. This will orphan the old classloader and the only references to the old classloader
     * are from the objects that it loaded.
     */
    private final Runnable refresher = new Runnable() {
      @Override
      public void run() {
        while (!executor.isTerminating()) {
          try {
            printDebug("Recreating delegate classloader due to filesystem change event");
            updateDelegateClassloader();
            return;
          } catch (Exception e) {
            e.printStackTrace();
            try {
              Thread.sleep(getMonitorInterval());
            } catch (InterruptedException ie) {
              ie.printStackTrace();
            }
          }
        }
      }
    };

    private final ThreadPoolExecutor executor;
    private final DefaultFileMonitor monitor;

    private Updater(ReloadingVFSClassLoader fileMonitor) {
      BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(2);
      ThreadFactory factory = r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
      };
      this.executor = new ThreadPoolExecutor(1, 1, 1, SECONDS, queue, factory);

      this.monitor = new DefaultFileMonitor(fileMonitor);
      monitor.setDelay(getMonitorInterval());
      monitor.setRecursive(false);
      monitor.start();
      printDebug("Monitor started with interval set to: " + monitor.getDelay());

    }

    private FileMonitor getMonitor() {
      return this.monitor;
    }

    private void scheduleRefresh() {
      try {
        this.executor.execute(refresher);
      } catch (RejectedExecutionException e) {
        printDebug("Ignoring refresh request (already refreshing)");
      }
    }

    private void shutdown() {
      this.executor.shutdownNow();
      this.monitor.stop();
    }

  }

  public ReloadingVFSClassLoader(ClassLoader parent) {
    super(ReloadingVFSClassLoader.class.getSimpleName(), parent);
    printDebug("Parent ClassLoader: " + parent.getClass().getName());
    this.name = ReloadingVFSClassLoader.class.getSimpleName();
    this.parent = parent;
    this.classpath = CLASSPATH;
    this.preDelegation = PRE_DELEGATION;
    this.monitorInterval = MONITOR_INTERVAL;
  }

  protected DefaultFileSystemManager getFileSystem() {
    if (null == this.vfs) {
      if (DEBUG) {
        VFSManager.enableDebug();
      }
      try {
        this.vfs = VFSManager.generateVfs();
      } catch (FileSystemException e) {
        printError("Error creating FileSystem: " + e.getMessage());
        e.printStackTrace();
      }
      printDebug("VFS File System created.");
    }
    return this.vfs;
  }

  protected String getClassPath() {
    return this.classpath;
  }

  protected boolean isPreDelegationModel() {
    return this.preDelegation;
  }

  protected long getMonitorInterval() {
    return this.monitorInterval;
  }

  private void addFileToMonitor(FileObject file) throws RuntimeException {
    try {
      updater.ifPresent(u -> {
        u.getMonitor().addFile(file);
      });
    } catch (RuntimeException re) {
      if (re.getMessage().contains("files-cache"))
        printDebug("files-cache error adding " + file.toString() + " to VFS monitor. "
            + "There is no implementation for files-cache in VFS2");
      else
        printDebug("Runtime error adding " + file.toString() + " to VFS monitor");

      re.printStackTrace();

      throw re;
    }
  }

  private synchronized void updateDelegateClassloader() throws Exception {
    try {
      updateLock.writeLock().lock();
      // Re-resolve the files on the classpath, things may have changed.
      long retries = 0;
      long currentSleepMillis = sleepInterval;
      FileObject[] classpathFiles = VFSManager.resolve(getFileSystem(), this.getClassPath());
      if (classpathFiles.length == 0) {
        while (classpathFiles.length == 0 && retryPermitted(retries)) {
          try {
            printDebug("VFS path was empty.  Waiting " + currentSleepMillis + " ms to retry");
            Thread.sleep(currentSleepMillis);
            classpathFiles = VFSManager.resolve(getFileSystem(), this.getClassPath());
            retries++;
            currentSleepMillis = Math.min(maxWaitInterval, currentSleepMillis + sleepInterval);
          } catch (InterruptedException e) {
            printError("VFS Retry Interruped");
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      }
      if (classpathFiles.length == 0) {
        printError("ReloadingVFSClassLoader has no resources on classpath");
      }
      this.files = classpathFiles;
      // There is a chance that the listener was removed from the top level directory or
      // its children if they were deleted within some time window. Re-add files to be
      // monitored. The Monitor will ignore files that are already/still being monitored.
      // forEachCatchRTEs will capture a stream of thrown exceptions.
      // and can collect them to list or reduce into one exception
      forEachCatchRTEs(Arrays.stream(this.files), f -> {
        addFileToMonitor(f);
        printDebug("monitoring: " + f.toString());
      });
      // Create the new classloader delegate
      printDebug("Rebuilding dynamic classloader using files: " + stringify(this.files));
      VFSClassLoaderWrapper cl;
      if (this.isPreDelegationModel()) {
        // This is the normal classloader parent delegation model
        cl = new VFSClassLoaderWrapper(this.files, getFileSystem(), parent);
      } else {
        // This delegates to the parent after we lookup locally first.
        cl = new VFSClassLoaderWrapper(this.files, getFileSystem(), parent) {
          @Override
          public synchronized Class<?> loadClass(String name, boolean resolve)
              throws ClassNotFoundException {
            // Check to see if this ClassLoader has already loaded the class
            Class<?> c = findLoadedClass(name);
            if (c != null) {
              printDebug("Returning loaded class: " + name);
              return c;
            }
            try {
              // try finding this class here instead of parent
              return findClass(name);
            } catch (ClassNotFoundException e) {

            }
            return super.loadClass(name, resolve);
          }
        };
      }
      // An HDFS FileSystem and Configuration object were created for each unique HDFS namespace
      // in the call to resolve above. The HDFS Client did us a favor and cached these objects
      // so that the next time someone calls FileSystem.get(uri), they get the cached object.
      // However, these objects were created not with the VFS classloader, but the
      // classloader above it. We need to override the classloader on the Configuration objects.
      // Ran into an issue were log recovery was being attempted and SequenceFile$Reader was
      // trying to instantiate the key class via WritableName.getClass(String, Configuration)
      printDebug("Setting ClassLoader on HDFS FileSystem objects");
      for (FileObject fo : this.files) {
        if (fo instanceof HdfsFileObject) {
          String uri = fo.getName().getRootURI();
          Configuration c = new Configuration(true);
          c.set(FileSystem.FS_DEFAULT_NAME_KEY, uri);
          try {
            FileSystem fs = FileSystem.get(c);
            fs.getConf().setClassLoader(cl);
          } catch (IOException e) {
            throw new RuntimeException("Error setting classloader on HDFS FileSystem object", e);
          }
        }
      }

      // Update the delegate reference to the new classloader
      this.cl = cl;
      printDebug("ReloadingVFSClassLoader set.");
    } finally {
      updateLock.writeLock().unlock();
    }
  }

  /**
   * Remove the file from the monitor
   *
   * @param file
   *          to remove
   * @throws RuntimeException
   *           if error
   */
  private void removeFile(FileObject file) throws RuntimeException {
    try {
      updater.ifPresent(u -> {
        u.getMonitor().removeFile(file);
      });
    } catch (RuntimeException re) {
      printError("Error removing file from VFS cache: " + file.toString());
      re.printStackTrace();
      throw re;
    }
  }

  @Override
  public void fileCreated(FileChangeEvent event) throws Exception {
    printDebug(event.getFileObject().getURL().toString() + " created, recreating classloader");
    updater.ifPresent(u -> u.scheduleRefresh());
  }

  @Override
  public void fileDeleted(FileChangeEvent event) throws Exception {
    printDebug(event.getFileObject().getURL().toString() + " deleted, recreating classloader");
    updater.ifPresent(u -> u.scheduleRefresh());
  }

  @Override
  public void fileChanged(FileChangeEvent event) throws Exception {
    printDebug(event.getFileObject().getURL().toString() + " changed, recreating classloader");
    updater.ifPresent(u -> u.scheduleRefresh());
  }

  @Override
  public void close() {

    forEachCatchRTEs(Stream.of(this.files), f -> {
      removeFile(f);
      printDebug("Closing, removing file from monitoring: " + f.toString());
    });

    updater.ifPresent(u -> u.shutdown());
    if (null != this.vfs)
      VFSManager.returnVfs(this.vfs);
    vfs = null;
  }

  public static <T> void forEachCatchRTEs(Stream<T> stream, Consumer<T> consumer) {
    stream.flatMap(o -> {
      try {
        consumer.accept(o);
        return null;
      } catch (RuntimeException e) {
        return Stream.of(e);
      }
    }).reduce((e1, e2) -> {
      e1.addSuppressed(e2);
      return e1;
    }).ifPresent(e -> {
      throw e;
    });
  }

  private boolean retryPermitted(long retries) {
    return (this.maxRetries < 0 || retries < this.maxRetries);
  }

  public String stringify(FileObject[] files) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    String delim = "";
    for (FileObject file : files) {
      sb.append(delim);
      delim = ", ";
      sb.append(file.getName());
    }
    sb.append(']');
    return sb.toString();
  }

  /**
   * Return a reference to the delegate classloader, create a new one if necessary
   *
   * @return reference to delegate classloader
   */
  synchronized ClassLoader getDelegateClassLoader() {
    // We cannot create the VFS file system during VM initialization,
    // we have to perform some lazy initialization here due to the fact
    // that the logging libraries (and others) make use of the ServiceLoader
    // and call ClassLoader.getSystemClassLoader() which you can't do until
    // the VM is fully initialized.
    if (!isVMInitialized() || vfsInitializing) {
      return this.parent;
    } else if (null == this.vfs) {
      this.vfsInitializing = true;
      printDebug("getDelegateClassLoader() initializing VFS.");
      getFileSystem();
      if (null == this.vfs) {
        // Some error happened
        throw new RuntimeException("Problem creating VFS file system");
      }
      printDebug("getDelegateClassLoader() VFS initialized.");
    }
    if (null == this.cl) {
      try {
        // If this is the system class loader, then reloading won't work.
        // If not the system class loader, start the file monitor.
        if (!isSystemClassLoader()) {
          printDebug("Reloading enabled");
          updater = Optional.of(new Updater(this));
        } else {
          printDebug("Reloading disabled as this is the java.system.class.loader");
        }
        printDebug("Creating initial delegate class loader");
        updateDelegateClassloader();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error creating initial delegate classloader", e);
      }
    }
    if (this.vfsInitializing) {
      this.vfsInitializing = false;
      printDebug(ClassPathPrinter.getClassPath(this, true));
    }
    try {
      updateLock.readLock().lock();
      return this.cl;
    } finally {
      updateLock.readLock().unlock();
    }
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    ClassLoader d = getDelegateClassLoader();
    if (d instanceof VFSClassLoaderWrapper) {
      return ((VFSClassLoaderWrapper) d).findClass(name);
    } else {
      return null;
    }
  }

  @Override
  public URL findResource(String name) {
    ClassLoader d = getDelegateClassLoader();
    if (d instanceof VFSClassLoaderWrapper) {
      return ((VFSClassLoaderWrapper) d).findResource(name);
    } else {
      return null;
    }
  }

  @Override
  public Enumeration<URL> findResources(String name) throws IOException {
    ClassLoader d = getDelegateClassLoader();
    if (d instanceof VFSClassLoaderWrapper) {
      return ((VFSClassLoaderWrapper) d).findResources(name);
    } else {
      return null;
    }
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    ClassLoader d = getDelegateClassLoader();
    if (d instanceof VFSClassLoaderWrapper) {
      return ((VFSClassLoaderWrapper) d).loadClass(name, resolve);
    } else {
      return null;
    }
  }

  @Override
  public String getName() {
    return name;
  }

  private boolean isSystemClassLoader() {
    return ClassLoader.getSystemClassLoader().equals(this);
  }

  protected boolean isVMInitialized() {
    if (VM_INITIALIZED) {
      return VM_INITIALIZED;
    } else {
      // We can't call VM.isBooted() directly, but we know from System.initPhase3() that
      // when this classloader is set via 'java.system.class.loader' that it will be initialized,
      // then set as the Thread context classloader, then the VM is fully initialized.
      try {
        printDebug(
            "System ClassLoader: " + ClassLoader.getSystemClassLoader().getClass().getName());
        VM_INITIALIZED = isSystemClassLoader();
      } catch (IllegalStateException e) {
        // VM is still initializing
        VM_INITIALIZED = false;
      }
      printDebug("VM Initialized: " + VM_INITIALIZED);
      return VM_INITIALIZED;
    }
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return getDelegateClassLoader().loadClass(name);
  }

  @Override
  public URL getResource(String name) {
    return getDelegateClassLoader().getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    return getDelegateClassLoader().getResources(name);
  }

  @Override
  public Stream<URL> resources(String name) {
    return getDelegateClassLoader().resources(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    return getDelegateClassLoader().getResourceAsStream(name);
  }

  @Override
  public void setDefaultAssertionStatus(boolean enabled) {
    getDelegateClassLoader().setDefaultAssertionStatus(enabled);
  }

  @Override
  public void setPackageAssertionStatus(String packageName, boolean enabled) {
    getDelegateClassLoader().setPackageAssertionStatus(packageName, enabled);
  }

  @Override
  public void setClassAssertionStatus(String className, boolean enabled) {
    getDelegateClassLoader().setClassAssertionStatus(className, enabled);
  }

  @Override
  public void clearAssertionStatus() {
    getDelegateClassLoader().clearAssertionStatus();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((parent.getName() == null) ? 0 : parent.getName().hashCode());
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
    ReloadingVFSClassLoader other = (ReloadingVFSClassLoader) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (parent == null) {
      if (other.parent != null)
        return false;
    } else if (!parent.getName().equals(other.parent.getName()))
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();

    if (null != this.files) {
      for (FileObject f : files) {
        try {
          buf.append("\t").append(f.getURL()).append("\n");
        } catch (FileSystemException e) {
          printError("Error getting URL for file: " + f.toString());
          e.printStackTrace();
        }
      }
    }
    return buf.toString();
  }

  // VisibleForTesting intentionally not using annotation from Guava
  // because it adds unwanted dependency
  void setMaxRetries(long maxRetries) {
    this.maxRetries = maxRetries;
  }

  // VisibleForTesting intentionally not using annotation from Guava
  // because it adds unwanted dependency
  void setVMInitializedForTests() {
    VM_INITIALIZED = true;
  }

  // VisibleForTesting intentionally not using annotation from Guava
  // because it adds unwanted dependency
  void setVFSForTests(DefaultFileSystemManager vfs) {
    this.vfs = vfs;
  }

  void enableDebugForTests() {
    DEBUG = true;
  }
}
