/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

/**
 * Utility method for working with threads.
 */
public final class ThreadUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadUtils.class);
  private static final ThreadMXBean THREAD_BEAN =
      ManagementFactory.getThreadMXBean();

  /**
   * @param thread a thread
   * @return a human-readable representation of the thread's stack trace
   */
  public static String formatStackTrace(Thread thread) {
    Throwable t = new Throwable(String.format("Stack trace for thread %s (State: %s):",
        thread.getName(), thread.getState()));
    t.setStackTrace(thread.getStackTrace());
    return formatStackTrace(t);
  }

  /**
   * Return formatted stacktrace of Throwable instance.
   * @param t - a Throwable instance
   * @return a human-readable representation of the Throwable's stack trace
   */
  public static String formatStackTrace(Throwable t) {
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  /**
   * Logs a stack trace for all threads currently running in the JVM, similar to jstack.
   */
  public static void logAllThreads() {
    logThreadInfo(LOG, "Dumping all threads:", 0);
  }

  /**
   * From https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
   *
   * The following method shuts down an ExecutorService in two phases, first by calling shutdown to
   * reject incoming tasks, and then calling shutdownNow, if necessary, to cancel any lingering
   * tasks.
   *
   * @param pool the executor service to shutdown
   * @param timeoutMs how long to wait for the service to shut down
   */
  public static void shutdownAndAwaitTermination(ExecutorService pool, long timeoutMs) {
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(timeoutMs / 2, TimeUnit.MILLISECONDS)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(timeoutMs / 2, TimeUnit.MILLISECONDS)) {
          LOG.warn("Pool did not terminate");
        }
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
    }
  }

  /**
   * @return a string representation of the current thread
   */
  public static String getCurrentThreadIdentifier() {
    return getThreadIdentifier(Thread.currentThread());
  }

  /**
   * @param thread the thread
   * @return a string representation of the given thread
   */
  public static String getThreadIdentifier(Thread thread) {
    return String.format("%d(%s)", thread.getId(), thread.getName());
  }

  private static String getTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  /**
   * Prints the information and stack traces of all threads.
   *
   * @param stream the stream to
   * @param title  a string title for the stack trace
   */
  public static synchronized void printThreadInfo(PrintStream stream, String title) {
    stream.println("Process Thread Dump: " + title);
    stream.println(THREAD_BEAN.getThreadCount() + " active theads");
    for (ThreadInfo ti: THREAD_BEAN.dumpAllThreads(true, true)) {
      stream.print(ti.toString());
    }
    stream.flush();
  }

  /**
   * The value of the previous log time, unit is ms.
   */
  @GuardedBy("ThreadUtils.class")
  private static long sPreviousLogTime = 0;

  /**
   * Log the current thread stacks at INFO level.
   * @param log the logger that logs the stack trace
   * @param title a descriptive title for the call stacks
   * @param minInterval the minimum time from the last, unit is second
   */
  public static void logThreadInfo(Logger log,
      String title,
      long minInterval) {
    boolean dumpStack = false;
    if (log.isInfoEnabled()) {
      synchronized (ThreadUtils.class) {
        long now = System.nanoTime() / 1000000;
        if (now - sPreviousLogTime >= minInterval * 1000) {
          sPreviousLogTime = now;
          dumpStack = true;
        }
      }
      if (dumpStack) {
        try {
          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          printThreadInfo(new PrintStream(buffer, false, "UTF-8"), title);
          log.info(buffer.toString(Charset.defaultCharset().name()));
        } catch (UnsupportedEncodingException ignored) {
          // swallow the nit exception to keep silent.
        }
      }
    }
  }

  private ThreadUtils() {} // prevent instantiation of utils class
}
