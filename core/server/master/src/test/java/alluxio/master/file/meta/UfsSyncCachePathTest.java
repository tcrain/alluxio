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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UfsSyncCachePathTest {

  private AlluxioURI mGrandParentDir;
  private AlluxioURI mParentPath;
  private AlluxioURI mChildPath;
  private AlluxioURI mChildFile;
  private UfsSyncPathCache mUspCache;

  @Before
  public void before() throws Exception {
    mGrandParentDir = new AlluxioURI("/dir1");
    mParentPath = new AlluxioURI("/dir1/dir2");
    mChildPath = new AlluxioURI("/dir1/dir2/dir3");
    mChildFile = new AlluxioURI("/dir1/dir2/file");
    mUspCache = new UfsSyncPathCache();
  }

  @Test
  public void ignoreIntervalTime() {
    // request from getFileInfo
    boolean shouldSync = mUspCache.shouldSyncPath(mParentPath, -1, DescendantType.ONE);
    Assert.assertFalse(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 0, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
    // request from listStatus
    shouldSync = mUspCache.shouldSyncPath(mParentPath, -1, DescendantType.ALL);
    Assert.assertFalse(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 0, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
  }

  /**
   * The path itself is in UfsSyncCachePath.
   *
   * @throws Exception
   */
  @Test
  public void getFileInfoInCache() throws Exception {
    getFileInfoInCache(DescendantType.NONE);
    getFileInfoInCache(DescendantType.ONE);
    getFileInfoInCache(DescendantType.ALL);
  }

  private void getFileInfoInCache(DescendantType descendantType) throws Exception {
    mUspCache.notifySyncedPath(mParentPath, descendantType, mUspCache.startSync(mParentPath));
    Thread.sleep(50);
    // request from getFileInfo
    boolean shouldSync = mUspCache.shouldSyncPath(mParentPath, 30, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 10000, DescendantType.ONE);
    Assert.assertFalse(shouldSync);
  }

  /**
   * The direct parent dir of path is in UfsSyncCachePath.
   *
   * @throws Exception
   */
  @Test
  public void getFileInfoFromDirectParent() throws Exception {
    getFileInfoFromDirectParent(DescendantType.NONE);
    getFileInfoFromDirectParent(DescendantType.ONE);
    getFileInfoFromDirectParent(DescendantType.ALL);
  }

  private void getFileInfoFromDirectParent(DescendantType descendantType) throws Exception {
    mUspCache.notifySyncedPath(mParentPath, descendantType, mUspCache.startSync(mParentPath));
    Thread.sleep(50);
    // test child directory
    boolean shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ONE);
    Assert.assertFalse(shouldSync);
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ONE);
    Assert.assertFalse(shouldSync);
  }

  /**
   * The grand parent dir of path is in UfsSyncCachePath and the descendantType is ONE.
   *
   * @throws Exception
   */
  @Test
  public void getFileInfoFromGrandParentONE() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ONE,
        mUspCache.startSync(mGrandParentDir));
    Thread.sleep(50);
    // test child directory
    boolean shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
  }

  /**
   * The grand parent dir of path is in UfsSyncCachePath and the descendantType is ALL.
   *
   * @throws Exception
   */
  @Test
  public void getFileInfoFromGrandParentALL() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ALL,
        mUspCache.startSync(mGrandParentDir));
    Thread.sleep(50);
    // test child directory
    boolean shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ONE);
    Assert.assertFalse(shouldSync);
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ONE);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ONE);
    Assert.assertFalse(shouldSync);
  }

  /**
   * The path itself is in UfsSyncCachePath.
   *
   * @throws Exception
   */
  @Test
  public void listStatusInCache() throws Exception {
    listStatusInCache(DescendantType.NONE);
    listStatusInCache(DescendantType.ONE);
    listStatusInCache(DescendantType.ALL);
  }

  private void listStatusInCache(DescendantType descendantType) throws Exception {
    mUspCache.notifySyncedPath(mParentPath, descendantType, mUspCache.startSync(mParentPath));
    Thread.sleep(50);
    // request from listStatus
    boolean shouldSync = mUspCache.shouldSyncPath(mParentPath, 30, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mParentPath, 10000, DescendantType.ALL);
    Assert.assertFalse(shouldSync);
  }

  /**
   * The direct parent dir of path is in UfsSyncCachePath and the descendantType is ONE.
   *
   * @throws Exception
   */
  @Test
  public void lsFromDirectParentONE() throws Exception {
    mUspCache.notifySyncedPath(mParentPath, DescendantType.ONE, mUspCache.startSync(mParentPath));
    Thread.sleep(50);
    // test child directory
    boolean shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
  }

  /**
   * The direct parent dir of path is in UfsSyncCachePath and the descendantType is ALL.
   *
   * @throws Exception
   */
  @Test
  public void lsFromDirectParentALL() throws Exception {
    mUspCache.notifySyncedPath(mParentPath, DescendantType.ALL, mUspCache.startSync(mParentPath));
    Thread.sleep(50);
    // test child directory
    boolean shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ALL);
    Assert.assertFalse(shouldSync);
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ALL);
    Assert.assertFalse(shouldSync);
  }

  /**
   * The grand parent dir of path is in UfsSyncCachePath and the descendantType is ONE.
   *
   * @throws Exception
   */
  @Test
  public void lsFromGrandParentONE() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ONE, mUspCache.startSync(mGrandParentDir));
    Thread.sleep(50);
    // test child directory
    boolean shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
  }

  /**
   * The grand parent dir of path is in UfsSyncCachePath and the descendantType is ALL.
   *
   * @throws Exception
   */
  @Test
  public void lsFromGrandParentALL() throws Exception {
    mUspCache.notifySyncedPath(mGrandParentDir, DescendantType.ALL,
        mUspCache.startSync(mGrandParentDir));
    Thread.sleep(50);
    // test child directory
    boolean shouldSync = mUspCache.shouldSyncPath(mChildPath, 30, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildPath, 10000, DescendantType.ALL);
    Assert.assertFalse(shouldSync);
    // test child file
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 40, DescendantType.ALL);
    Assert.assertTrue(shouldSync);
    shouldSync = mUspCache.shouldSyncPath(mChildFile, 10000, DescendantType.ALL);
    Assert.assertFalse(shouldSync);
  }
}
