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

package alluxio.client.fs;

import static alluxio.testutils.CrossClusterTestUtils.CREATE_DIR_OPTIONS;
import static alluxio.testutils.CrossClusterTestUtils.CREATE_OPTIONS;
import static alluxio.testutils.CrossClusterTestUtils.assertFileDoesNotExist;
import static alluxio.testutils.CrossClusterTestUtils.checkClusterSyncAcrossAll;
import static alluxio.testutils.CrossClusterTestUtils.fileExists;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.ListStatusPOptions;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Stopwatch;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CrossClusterRootMountIntegrationTest {

  private String mUfsUri1;
  private UnderFileSystem mLocalUfs;

  private final AlluxioURI mMountPoint1 = new AlluxioURI("/");

  private FileSystemCrossCluster mClient1;

  private FileSystemCrossCluster mClient2;
  public static final int SYNC_TIMEOUT = 10_000;

  WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(SYNC_TIMEOUT);

  @Rule(order = 0)
  public TemporaryFolder mFolder = new TemporaryFolder();

  String mRootUfs = AlluxioTestDirectory
      .createTemporaryDirectory("FileSystemMasterTest").getAbsolutePath();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource1 =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_CROSS_CLUSTER, true)
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, 1)
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE, 1)
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE, true)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mRootUfs)
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setStartCluster(false).includeCrossClusterStandalone().build();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource2 =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_CROSS_CLUSTER, true)
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, 1)
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE, 1)
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE, true)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mRootUfs)
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c2")
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setStartCluster(false).build();

  @Before
  public void before() throws Exception {
    String ufsPath1 = mFolder.newFolder().getAbsoluteFile().toString();
    mUfsUri1 = "file://" + ufsPath1;

    mLocalUfs = new LocalUnderFileSystemFactory().create(mFolder.getRoot().getAbsolutePath(),
        UnderFileSystemConfiguration.defaults(Configuration.global()));

    // same ufs root mount point
    mLocalAlluxioClusterResource1.setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mUfsUri1);
    mLocalAlluxioClusterResource2.setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mUfsUri1);

    IntegrationTestUtils.reserveMasterPorts();
    mLocalAlluxioClusterResource1.start();
    IntegrationTestUtils.reserveMasterPorts();
    mLocalAlluxioClusterResource2.start();

    mClient1 = mLocalAlluxioClusterResource1.getCrossClusterClient();
    mClient2 = mLocalAlluxioClusterResource2.getCrossClusterClient();
  }

  @Test
  public void crossClusterWrite() throws Exception {

    Stopwatch sw = Stopwatch.createStarted();

    AlluxioURI file1 = mMountPoint1.join("file1");

    // Perform a recursive sync so that the invalidation cache is up-to-date
    mClient1.listStatus(mMountPoint1, ListStatusPOptions.newBuilder()
        .setRecursive(true).build());
    mClient2.listStatus(mMountPoint1, ListStatusPOptions.newBuilder()
        .setRecursive(true).build());
    assertFileDoesNotExist(file1, mClient1, mClient2);

    mClient1.createFile(file1, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, mClient1, mClient2),
        mWaitOptions);

    // delete the file outside alluxio, be sure we do not see the update
    mLocalUfs.deleteFile(PathUtils.concatPath(mUfsUri1, "file1"));
    Assert.assertThrows(TimeoutException.class,
        () -> CommonUtils.waitFor("File removed outside of alluxio",
            () -> !fileExists(file1, mClient1, mClient2),
            mWaitOptions));
    // now delete the file in alluxio
    mClient2.delete(file1);
    CommonUtils.waitFor("File synced across clusters",
        () -> !fileExists(file1, mClient1, mClient2),
        mWaitOptions);

    // create a directory
    AlluxioURI dir1 = mMountPoint1.join("/dir1");
    assertFileDoesNotExist(dir1, mClient1, mClient2);
    mClient1.createDirectory(dir1, CREATE_DIR_OPTIONS);
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(dir1, mClient1, mClient2),
        mWaitOptions);

    // be sure files are synced in both directions
    checkClusterSyncAcrossAll(mMountPoint1, mClient1, mClient2);

    System.out.println("Took: " + sw.elapsed(TimeUnit.MILLISECONDS));
  }
}
