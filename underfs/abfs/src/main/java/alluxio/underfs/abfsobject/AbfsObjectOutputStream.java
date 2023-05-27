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

import alluxio.underfs.ContentHashable;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing a file into Azure Blob Storage. The data will be persisted to
 * a temporary directory on the local disk and copied as a complete file when the
 * {@link #close()} method is called.
 */
@NotThreadSafe
public final class AbfsObjectOutputStream extends OutputStream implements ContentHashable {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsObjectOutputStream.class);

  private final String mKey;
  /**
   * The local file that will be uploaded when the stream is closed.
   */
  private final File mFile;

  private final BlobContainerClient mClient;

  private final OutputStream mLocalOutputStream;

  private final AtomicBoolean mClosed = new AtomicBoolean(false);

  private String mContentHash;

  /**
   * Creates a name instance of {@link AbfsObjectOutputStream}.
   *
   * @param key the key of the file
   * @param client the OBS client
   * @param tmpDirs a list of temporary directories
   */
  public AbfsObjectOutputStream(
      String key, BlobContainerClient client, List<String> tmpDirs) throws IOException {
    Preconditions.checkArgument(key != null && !key.isEmpty(),
        "Object key must not be null or empty.");
    Preconditions.checkArgument(client != null, "client must not be null.");
    mKey = key;
    mClient = client;
    mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(tmpDirs), UUID.randomUUID()));
    mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
  }

  /**
   * Writes the given bytes to this output stream. Before close, the bytes are all written to local
   * file.
   *
   * @param b the bytes to write
   */
  @Override
  public void write(int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  /**
   * Writes the given byte array to this output stream. Before close, the bytes are all written to
   * local file.
   *
   * @param b the byte array
   */
  @Override
  public void write(byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  /**
   * Writes the given number of bytes from the given byte array starting at the given offset to this
   * output stream. Before close, the bytes are all written to local file.
   *
   * @param b the byte array
   * @param off the start offset in the data
   * @param len the number of bytes to write
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalOutputStream.write(b, off, len);
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be written out. Before
   * close, the data are flushed to local file.
   */
  @Override
  public void flush() throws IOException {
    mLocalOutputStream.flush();
  }

  /**
   * Closes this output stream. When an output stream is closed, the local temporary file is
   * uploaded to the Azure Blob Service. Once the file is uploaded, the temporary file is deleted.
   */
  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      LOG.warn("AbfsObjectOutputStream is already closed");
      return;
    }
    mLocalOutputStream.close();
    try {
      mContentHash = mClient.getBlobClient(mKey).uploadFromFileWithResponse(
              new BlobUploadFromFileOptions(mFile.getPath()), null, Context.NONE)
          .getValue().getETag();
    } catch (UncheckedIOException e) {
      LOG.error("Failed to upload {}. Temporary file @ {}", mKey, mFile.getPath());
      throw new IOException(e);
    } catch (BlobStorageException e) {
      throw AlluxioAzureException.from(e);
    } finally {
      // Delete the temporary file on the local machine if the Azure client completed the
      // upload or if the upload failed.
      if (!mFile.delete()) {
        LOG.error("Failed to delete temporary file @ {}", mFile.getPath());
      }
    }
  }

  @Override
  public Optional<String> getContentHash() {
    return Optional.ofNullable(mContentHash);
  }
}
