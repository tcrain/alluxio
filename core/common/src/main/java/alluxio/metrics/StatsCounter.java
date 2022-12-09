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

package alluxio.metrics;

import com.codahale.metrics.Counter;

import java.util.concurrent.TimeUnit;

/**
 * Implementation of StatsCounter similar to the one in
 * {@link com.google.common.cache.AbstractCache}.
 */
public final class StatsCounter {
  private final Counter mHitCount;
  private final Counter mMissCount;
  private final Counter mTotalLoadTime;
  private final Counter mEvictionCount;
  private final MetricKey mEvictionTimer;

  /**
   * Creates a new {@link StatsCounter} instance.
   * @param evictionTimerKey metric key for the eviction timer
   * @param evictionsKey metrics key for the eviction counter
   * @param hitsKey metrics key for the cache hits
   * @param loadTimesKey metrics key the time to load keys
   * @param missesKey metrics key for the cache misses
   * @param hitRatioKey metrics key for the hit to miss ratio
   */
  public StatsCounter(MetricKey evictionTimerKey, MetricKey evictionsKey,
      MetricKey hitsKey, MetricKey loadTimesKey, MetricKey missesKey,
      MetricKey hitRatioKey) {
    mHitCount = MetricsSystem.counter(hitsKey.getName());
    mMissCount = MetricsSystem.counter(missesKey.getName());
    mTotalLoadTime = MetricsSystem.counter(loadTimesKey.getName());
    mEvictionCount = MetricsSystem.counter(evictionsKey.getName());
    mEvictionTimer = evictionTimerKey;
    MetricsSystem.registerGaugeIfAbsent(hitRatioKey.getName(),
        () -> mHitCount.getCount() * 1.0
            / (mHitCount.getCount() + mMissCount.getCount()));
  }

  /**
   * Record a cache hit.
   */
  public void recordHit() {
    mHitCount.inc();
  }

  /**
   * Record a cache miss.
   */
  public void recordMiss() {
    mMissCount.inc();
  }

  /**
   * Record a loading of data asa result of cache miss.
   * @param loadTime amount of time it took to load data in nanoseconds
   */
  public void recordLoad(long loadTime) {
    mTotalLoadTime.inc(loadTime);
  }

  /**
   * Record evictions in the cache.
   * @param evictionCount the number of evictions
   * @param evictionTimeNanos time to perform the eviction traversal in nanoseconds
   */
  public void recordEvictions(long evictionCount, long evictionTimeNanos) {
    mEvictionCount.inc(evictionCount);
    MetricsSystem.timer(mEvictionTimer.getName()).update(evictionTimeNanos, TimeUnit.NANOSECONDS);
  }
}
