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
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.util.UnderFileSystemUtils;

import com.azure.core.exception.HttpResponseException;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.google.common.base.Preconditions;

import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link AbfsObjectUnderFileSystemFactory}.
 */
@ThreadSafe
public class AbfsObjectUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link AbfsObjectUnderFileSystemFactory}.
   */
  public AbfsObjectUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    AlluxioURI uri = new AlluxioURI(path);
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    String[] parts = bucketName.split("@");
    if (parts.length != 2) {
      throw new InvalidArgumentRuntimeException(String.format("Invalid uri %s", uri));
    }
    String[] bucketParts = parts[1].split("\\.");
    if (bucketParts.length < 1) {
      throw new InvalidArgumentRuntimeException(String.format("Invalid bucket name %s", parts[1]));
    }
    BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder();
    DataLakeServiceClientBuilder dlClientBuilder = new DataLakeServiceClientBuilder();
    for (Map.Entry<String, Object> entry : conf.toMap().entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (PropertyKey.Template.UNDERFS_AZURE_ACCOUNT_KEY.matches(key)) {
        StorageSharedKeyCredential cred = new StorageSharedKeyCredential(bucketParts[0],
            (String) value);
        clientBuilder.credential(cred);
        dlClientBuilder.credential(cred);
      }
    }
    try {
      BlobContainerClient client = clientBuilder.endpoint(
              "https://" + parts[1])
          .buildClient()
          .createBlobContainerIfNotExists(parts[0]);
      if (conf.isSet(PropertyKey.ABS_CLIENT_HIERARCHICAL)) {
        DataLakeServiceClient dataLakeServiceClient = dlClientBuilder
            .endpoint(conf.getString(PropertyKey.ABS_CLIENT_HIERARCHICAL))
            .buildClient();
        DataLakeFileSystemClient fsClient = dataLakeServiceClient.getFileSystemClient(parts[0]);
        return new AbfsObjectDataLakeUnderFileSystem(
            uri, client, fsClient, bucketName, parts[0], conf);
      } else {
        return new AbfsObjectUnderFileSystem(uri, client, bucketName, parts[0], conf);
      }
    } catch (HttpResponseException e) {
      throw AlluxioAzureException.from(e);
    }
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null
        && path.startsWith(Constants.HEADER_ABFSO);
  }
}
