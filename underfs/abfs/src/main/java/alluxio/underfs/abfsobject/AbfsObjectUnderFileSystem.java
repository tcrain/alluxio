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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.core.util.IterableStream;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.CopyStatusType;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Azure blob storage {@link UnderFileSystem} implementation based on the object store interface.
 */
@ThreadSafe
public class AbfsObjectUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsObjectUnderFileSystem.class);

  /**
   * Suffix for an empty file to flag it as a directory.
   */
  private static final String FOLDER_SUFFIX = PATH_SEPARATOR;

  private final BlobContainerClient mClient;

  private final String mBucketName;
  final String mContainerName;

  final Duration mTimeout;

  /**
   * Constructor for {@link AbfsObjectUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param containerName the name of the user's container
   * @param conf configuration for this ufs
   */
  AbfsObjectUnderFileSystem(
      AlluxioURI uri, BlobContainerClient client, String bucketName,
      String containerName, UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mClient = client;
    mBucketName = bucketName;
    mContainerName = containerName;
    // currently, no timeout
    mTimeout = null;
  }

  @Override
  public String getUnderFSType() {
    return "abfso";
  }

  // Setting ABFS owner via Alluxio is not supported. This is a no-op.
  @Override
  public void setOwner(String path, String user, String group) {}

  // Setting ABFS mode via Alluxio is not supported. This is a no-op.
  @Override
  public void setMode(String path, short mode) {}

  @Override
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    try {
      SyncPoller<BlobCopyInfo, Void> poller = mClient.getBlobClient(dst)
          .beginCopy(mClient.getBlobClient(src).getBlobUrl(), Duration.ofSeconds(1));
      return poller.waitForCompletion().getValue().getCopyStatus() == CopyStatusType.SUCCESS;
    } catch (BlobStorageException e) {
      throw AlluxioAzureException.from(e);
    }
  }

  @Override
  protected String getChildName(String child, String parent) throws IOException {
    if (child.length() + 1 == parent.length() && parent.startsWith(child)) {
      return "";
    }
    return super.getChildName(child, parent);
  }

  @Override
  public boolean createEmptyObject(String key) {
    try {
      return mClient.getBlobClient(key).uploadWithResponse(
          new BlobParallelUploadOptions(BinaryData.fromBytes(EMPTY_BYTE_ARRAY)),
          mTimeout, Context.NONE).getStatusCode() == HttpStatus.SC_CREATED;
    } catch (BlobStorageException e) {
      throw AlluxioAzureException.from(e);
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new AbfsObjectOutputStream(key, mClient, mUfsConf.getList(PropertyKey.TMP_DIRS));
  }

  @Override
  protected boolean deleteObject(String key) {
    Response<Boolean> response = mClient.getBlobClient(key).deleteIfExistsWithResponse(
        null, null, mTimeout, Context.NONE);
    return response.getStatusCode() == HttpStatus.SC_ACCEPTED
        || response.getStatusCode() == HttpStatus.SC_NOT_FOUND;
  }

  @Override
  protected List<String> deleteObjects(List<String> keys) {
    List<String> urls = keys.stream().map(nxt -> mClient.getBlobClient(nxt).getBlobUrl())
        .collect(Collectors.toList());
    BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(mClient).buildClient();
    return blobBatchClient.deleteBlobs(urls, DeleteSnapshotsOptionType.INCLUDE).stream()
        .filter(response ->
            response.getStatusCode() == HttpStatus.SC_ACCEPTED).map(response ->
            response.getRequest().getUrl().getFile())
        .collect(Collectors.toList());
  }

  @Override
  protected String getFolderSuffix() {
    return FOLDER_SUFFIX;
  }

  @Override
  @Nullable
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive) {
    String delimiter = recursive ? "" : PATH_SEPARATOR;
    String normalizedKey = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix.
    normalizedKey = normalizedKey.equals(PATH_SEPARATOR) ? "" : normalizedKey;
    try {
      Iterator<PagedResponse<BlobItem>> pages = mClient.listBlobsByHierarchy(delimiter,
          new ListBlobsOptions().setPrefix(normalizedKey)
              .setMaxResultsPerPage(getListingChunkLength(mUfsConf)),
          mTimeout).iterableByPage().iterator();
      if (pages.hasNext()) {
        return new AzureObjectListingChunk(pages, key);
      }
      return null;
    } catch (BlobStorageException e) {
      throw AlluxioAzureException.from(e);
    }
  }

  private static final class AzureObjectListingChunk implements ObjectListingChunk {
    final Iterator<PagedResponse<BlobItem>> mPages;
    final IterableStream<BlobItem> mItems;
    final String mKey;

    AzureObjectListingChunk(Iterator<PagedResponse<BlobItem>> pages, @Nullable String key) {
      mPages = pages;
      mItems = mPages.next().getElements();
      mKey = key;
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      ObjectStatus[] result = mItems.stream().filter(Objects::nonNull)
          .map(nxt -> {
            String name = nxt.getName();
            if (nxt.isPrefix()) { // this is a directory
              name = CommonUtils.addSuffixIfMissing(nxt.getName(), PATH_SEPARATOR);
            }
            BlobItemProperties properties = nxt.getProperties();
            if (properties == null) {
              return new ObjectStatus(name, null, 0, null);
            }
            return new ObjectStatus(name, nxt.getProperties().getETag(),
                nxt.getProperties().getContentLength(),
                nxt.getProperties().getLastModified().toInstant().toEpochMilli());
          })
          .toArray(ObjectStatus[]::new);
      if (mKey != null && result.length == 1 && result[0].getName().equals(mKey)) {
        return new ObjectStatus[0];
      }
      return result;
    }

    @Override
    public String[] getCommonPrefixes() {
      return mItems.stream().filter(BlobItem::isPrefix).map(BlobItem::getName)
          .toArray(String[]::new);
    }

    @Override
    public ObjectListingChunk getNextChunk() {
      if (!mPages.hasNext()) {
        return null;
      }
      return new AzureObjectListingChunk(mPages, null);
    }
  }

  @Override
  @Nullable
  protected ObjectStatus getObjectStatus(String key) {
    try {
      BlobProperties properties =  mClient.getBlobClient(key).getProperties();
      return new ObjectStatus(key, properties.getETag(),
          properties.getBlobSize(),
          properties.getLastModified().toInstant().toEpochMilli());
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        return null;
      }
      throw AlluxioAzureException.from(e);
    }
  }

  // No ACL integration currently, returns default empty value
  @Override
  protected ObjectPermissions getPermissions() {
    return new ObjectPermissions("", "", Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_ABFSO + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options,
      RetryPolicy retryPolicy) {
    try {
      return mClient.getBlobClient(key).openInputStream(
          new BlobInputStreamOptions().setRange(
              new BlobRange(options.getOffset(), options.getLength() == Long.MAX_VALUE
                  ? null : options.getLength())));
    } catch (BlobStorageException e) {
      throw AlluxioAzureException.from(e);
    }
  }
}
