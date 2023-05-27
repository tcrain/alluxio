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

package alluxio.underfs.abfsobject;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.CommonUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import com.azure.core.exception.HttpResponseException;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.common.implementation.Constants;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.PathProperties;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An ABFS file system client that supports hierarchical name space.
 */
public class AbfsObjectDataLakeUnderFileSystem extends AbfsObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsObjectUnderFileSystemFactory.class);

  private final DataLakeFileSystemClient mFsClient;

  /**
   * Constructor for {@link AbfsObjectUnderFileSystem}.
   *
   * @param uri        the {@link AlluxioURI} for this UFS
   * @param client the blob file system client
   * @param fsClient the data lake client for the file system
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param containerName the name of the user's container
   * @param conf       configuration for this ufs
   */
  AbfsObjectDataLakeUnderFileSystem(
      AlluxioURI uri, BlobContainerClient client, DataLakeFileSystemClient fsClient,
      String bucketName, String containerName, UnderFileSystemConfiguration conf) {
    super(uri, client, bucketName, containerName, conf);
    mFsClient = fsClient;
  }

  protected String pathToFolder(String key) {
    // Strips the slash if it is the end of the key string. This is because the slash at
    // the end of the string is not part of the Object key.
    return CommonUtils.stripSuffixIfPresent(key, PATH_SEPARATOR);
  }

  //@Override
  //protected String getChildName(String child, String parent) throws IOException {
//    return super.getChildName(CommonUtils.stripSuffixIfPresent(child, PATH_SEPARATOR), parent);
//  }

  @Override
  @Nullable
  protected ObjectStatus getObjectStatus(String key) {
    try {
      PathProperties properties =  mFsClient.getFileClient(stripPrefixIfPresent(key))
          .getProperties();
      if (!properties.isDirectory() && key.endsWith(PATH_SEPARATOR)) {
        return null;
      }
      return new ObjectStatus(key, properties.getETag(),
          properties.getFileSize(),
          properties.getLastModified().toInstant().toEpochMilli());
    } catch (HttpResponseException e) {
      if (e.getResponse().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        return null;
      }
      throw AlluxioAzureException.from(e);
    }
  }

  @Override
  protected boolean mkdirsInternal(String path) {
    try {
      return mFsClient.createDirectoryIfNotExistsWithResponse(
          stripPrefixIfPresent(pathToFolder(path)), null, mTimeout, Context.NONE)
          .getStatusCode() == HttpStatus.SC_CREATED;
    } catch (HttpResponseException e) {
      throw AlluxioAzureException.from(e);
    }
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) {
    try {
      mFsClient.getDirectoryClient(stripPrefixIfPresent(pathToFolder(path)))
          .deleteWithResponse(options.isRecursive(), null, mTimeout, Context.NONE);
    } catch (HttpResponseException e) {
      if (e.getResponse().getStatusCode() == HttpStatus.SC_CONFLICT) {
        LOG.error("Unable to delete directory {} because is is non empty and is not recursive",
            path);
        return false;
      } else if (e.getResponse().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        LOG.error("Unable to delete directory {} because it does not exist", path);
        return false;
      } else {
        throw AlluxioAzureException.from(e);
      }
    }
    return true;
  }

  @Override
  public boolean renameDirectory(String src, String dst) {
    if (!isDirectory(src)) {
      return false;
    }
    try {
      mFsClient.getDirectoryClient(stripPrefixIfPresent(src))
          .renameWithResponse(mContainerName, stripPrefixIfPresent(dst), null,
              new DataLakeRequestConditions()
                  .setIfNoneMatch(Constants.HeaderConstants.ETAG_WILDCARD),
              null, Context.NONE);
    } catch (HttpResponseException e) {
      if (e.getResponse().getStatusCode() == HttpStatus.SC_CONFLICT) {
        LOG.error("Unable to rename {} to {} because destination already exists.", src, dst);
        return false;
      } else if (e.getResponse().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        LOG.error("Unable to rename directory {} because it does not exist", src);
        return false;
      } else {
        throw AlluxioAzureException.from(e);
      }
    }
    return true;
  }

  @Override
  public boolean renameFile(String src, String dst) {
    if (!isFile(src)) {
      return false;
    }
    try {
      mFsClient.getFileClient(stripPrefixIfPresent(src))
          .renameWithResponse(mContainerName, stripPrefixIfPresent(dst), null,
              new DataLakeRequestConditions()
                  .setIfNoneMatch(Constants.HeaderConstants.ETAG_WILDCARD),
              null, Context.NONE);
    } catch (HttpResponseException e) {
      if (e.getResponse().getStatusCode() == HttpStatus.SC_CONFLICT) {
        LOG.error("Unable to rename {} to {} because destination already exists.", src, dst);
        return false;
      } else if (e.getResponse().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        LOG.error("Unable to rename file {} because it does not exist", src);
        return false;
      } else {
        throw AlluxioAzureException.from(e);
      }
    }
    return true;
  }

  @Override
  public boolean isFile(String path) {
    try {
      return !isRoot(path) && !mFsClient.getFileClient(stripPrefixIfPresent(path))
          .getProperties().isDirectory();
    } catch (HttpResponseException e) {
      if (e.getResponse().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        return false;
      } else {
        throw AlluxioAzureException.from(e);
      }
    }
  }

  @Override
  public boolean isDirectory(String path) {
    // Root is always a folder
    if (isRoot(path)) {
      return true;
    }
    try {
      return mFsClient.getDirectoryClient(pathToFolder(
          stripPrefixIfPresent(path))).getProperties().isDirectory();
    } catch (HttpResponseException e) {
      if (e.getResponse().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        return false;
      } else {
        throw AlluxioAzureException.from(e);
      }
    }
  }
}
