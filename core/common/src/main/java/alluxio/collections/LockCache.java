package alluxio.collections;

import alluxio.concurrent.LockMode;
import alluxio.resource.LockResource;
import alluxio.resource.RWLockResource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Lock cache.
 * @param <K> key type
 */
public class LockCache<K> implements Closeable {

  private final Cache<K, ReentrantReadWriteLock> mCache = CacheBuilder.newBuilder().weakValues()
      .build();

  /**
   * Lock cache.
   */
  public LockCache() {
  }

  /**
   * Locks the specified key in the specified mode.
   *
   * @param key the key to lock
   * @param mode the mode to lock in
   * @return a lock resource which must be closed to unlock the key
   */
  public LockResource get(K key, LockMode mode) {
    return get(key, mode, false);
  }

  /**
   * Locks the specified key in the specified mode.
   *
   * @param key the key to lock
   * @param mode the mode to lock in
   * @param useTryLock Determines whether or not to use {@link Lock#tryLock()} or
   *                   {@link Lock#lock()} to acquire the lock. Differs from
   *                   {@link #tryGet(Object, LockMode)} in that it will block until the lock has
   *                   been acquired.
   * @return a lock resource which must be closed to unlock the key
   */
  public RWLockResource get(K key, LockMode mode, boolean useTryLock) {
    return new RWLockResource(getRawReadWriteLock(key), mode, true, useTryLock);
  }

  /**
   * Attempts to take a lock on the given key.
   *
   * @param key the key to lock
   * @param mode lockMode to acquire
   * @return either empty or a lock resource which must be closed to unlock the key
   */
  public Optional<RWLockResource> tryGet(K key, LockMode mode) {
    ReentrantReadWriteLock lock = getRawReadWriteLock(key);
    Lock innerLock;
    switch (mode) {
      case READ:
        innerLock = lock.readLock();
        break;
      case WRITE:
        innerLock = lock.writeLock();
        break;
      default:
        throw new IllegalStateException("Unknown lock mode: " + mode);
    }
    if (!innerLock.tryLock()) {
      return Optional.empty();
    }
    return Optional.of(new RWLockResource(lock, mode, false, false));
  }

  /**
   * Get the raw readwrite lock from the pool.
   *
   * @param key key to look up the value
   * @return the lock associated with the key
   */
  @VisibleForTesting
  public ReentrantReadWriteLock getRawReadWriteLock(K key) {
    try {
      return mCache.get(key, ReentrantReadWriteLock::new);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("size", mCache.size())
        .toString();
  }

  /**
   * Returns whether the pool contains a particular key.
   *
   * @param key the key to look up in the pool
   * @return true if the key is contained in the pool
   */
  @VisibleForTesting
  public boolean containsKey(K key) {
    Preconditions.checkNotNull(key, "key can not be null");
    return mCache.getIfPresent(key) != null;
  }

  /**
   * @return the size of the pool
   */
  public int size() {
    return (int) mCache.size();
  }

  /**
   * @return all entries in the pool, for debugging purposes
   */
  @VisibleForTesting
  public Map<K, ReentrantReadWriteLock> getEntryMap() {
    return new HashMap<>(mCache.asMap());
  }

  @Override
  public void close() {
    mCache.cleanUp();
  }
}
