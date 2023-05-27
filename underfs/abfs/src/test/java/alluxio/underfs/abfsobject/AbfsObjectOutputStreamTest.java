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

import static org.mockito.ArgumentMatchers.any;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Duration;

/**
 * Unit tests for the {@link AbfsObjectOutputStream}.
 */
@RunWith(PowerMockRunner.class)
public class AbfsObjectOutputStreamTest {
  private static AlluxioConfiguration sConf = Configuration.global();

  private BlobContainerClient mClient;
  private BlobClient mBlobClient;
  private File mFile;
  private BufferedOutputStream mLocalOutputStream;

  /**
   * Sets the properties and configuration before each test runs.
   */
  @Before
  public void before() {
    mClient = Mockito.mock(BlobContainerClient.class);
    mBlobClient = Mockito.mock(BlobClient.class);
    Response<BlockBlobItem> response = Mockito.mock(Response.class);
    Mockito.when(response.getValue()).thenReturn(
        new BlockBlobItem("tag", null, null, false, ""));
    Mockito.when(mClient.getBlobClient(any(String.class))).thenReturn(mBlobClient);
    Mockito.when(mBlobClient.uploadFromFileWithResponse(any(BlobUploadFromFileOptions.class),
        any(), any())).thenReturn(response);
    mFile = Mockito.mock(File.class);
    Mockito.when(mFile.getPath()).thenReturn("somePath");
    Mockito.when(mFile.delete()).thenReturn(true);
    mLocalOutputStream = Mockito.mock(BufferedOutputStream.class);
  }

  /**
   * Tests to ensure IOException is thrown if {@link FileOutputStream}() throws an IOException.
   */
  @Test
  @PrepareForTest(AbfsObjectOutputStream.class)
  public void testConstructor() throws Exception {
    PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
    String errorMessage = "protocol doesn't support output";
    PowerMockito.whenNew(FileOutputStream.class).withArguments(mFile)
        .thenThrow(new IOException(errorMessage));
    Assert.assertThrows(errorMessage, IOException.class, () ->
        new AbfsObjectOutputStream("testKey", mClient,
        sConf.getList(PropertyKey.TMP_DIRS)).close());
  }

  /**
   * Tests to ensure {@link AbfsObjectOutputStream#write(int)} calls {@link OutputStream#write(int)}.
   */
  @Test
  @PrepareForTest(AbfsObjectOutputStream.class)
  public void testWrite1() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    AbfsObjectOutputStream stream = new AbfsObjectOutputStream("testKey", mClient,
        sConf.getList(PropertyKey.TMP_DIRS));
    stream.write(1);
    stream.close();
    Mockito.verify(mLocalOutputStream).write(1);
  }

  /**
   * Tests to ensure {@link AbfsObjectOutputStream#write(byte[], int, int)} calls
   * {@link OutputStream#write(byte[], int, int)} .
   */
  @Test
  @PrepareForTest(AbfsObjectOutputStream.class)
  public void testWrite2() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    AbfsObjectOutputStream stream = new AbfsObjectOutputStream("testKey", mClient,
        sConf.getList(PropertyKey.TMP_DIRS));
    byte[] b = new byte[1];
    stream.write(b, 0, 1);
    stream.close();
    Mockito.verify(mLocalOutputStream).write(b, 0, 1);
  }

  /**
   * Tests to ensure {@link AbfsObjectOutputStream#write(byte[])} calls {@link OutputStream#write(byte[])}.
   */
  @Test
  @PrepareForTest(AbfsObjectOutputStream.class)
  public void testWrite3() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    AbfsObjectOutputStream stream = new AbfsObjectOutputStream("testKey", mClient,
        sConf.getList(PropertyKey.TMP_DIRS));
    byte[] b = new byte[1];
    stream.write(b);
    stream.close();
    Mockito.verify(mLocalOutputStream).write(b, 0, 1);
  }

  /**
   * Tests to ensure IOException is thrown if
   * {@link BlobClient#uploadFromFileWithResponse(BlobUploadFromFileOptions, Duration, Context)}
   * throws an {@link AlluxioAzureException}.
   */
  @Test
  @PrepareForTest(AbfsObjectOutputStream.class)
  public void testCloseError() throws Exception {
    String errorMessage = "Invoke the createEmptyObject method error.";
    BufferedInputStream inputStream = PowerMockito.mock(BufferedInputStream.class);
    PowerMockito.whenNew(BufferedInputStream.class)
        .withArguments(any(FileInputStream.class)).thenReturn(inputStream);
    PowerMockito.when(mBlobClient.uploadFromFileWithResponse(any(BlobUploadFromFileOptions.class),
        any(), any())).thenThrow(
            new UncheckedIOException(new IOException(errorMessage)));

    Assert.assertThrows(errorMessage, IOException.class, () -> {
      AbfsObjectOutputStream stream = new AbfsObjectOutputStream(
          "testKey", mClient, sConf.getList(PropertyKey.TMP_DIRS));
      stream.close();
    });
  }

  /**
   * Tests to ensure {@link File#delete()} is called when close the stream.
   */
  @Test
  @PrepareForTest(AbfsObjectOutputStream.class)
  public void testCloseSuccess() throws Exception {
    PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
    FileOutputStream outputStream = PowerMockito.mock(FileOutputStream.class);
    PowerMockito.whenNew(FileOutputStream.class).withArguments(mFile).thenReturn(outputStream);
    FileInputStream inputStream = PowerMockito.mock(FileInputStream.class);
    PowerMockito.whenNew(FileInputStream.class).withArguments(mFile).thenReturn(inputStream);

    AbfsObjectOutputStream stream = new AbfsObjectOutputStream(
        "testKey", mClient, sConf.getList(PropertyKey.TMP_DIRS));
    stream.close();
    Mockito.verify(mFile).delete();
  }

  /**
   * Tests to ensure {@link AbfsObjectOutputStream#flush()} calls {@link OutputStream#flush()}.
   */
  @Test
  @PrepareForTest(AbfsObjectOutputStream.class)
  public void testFlush() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    AbfsObjectOutputStream stream = new AbfsObjectOutputStream(
        "testKey", mClient, sConf.getList(PropertyKey.TMP_DIRS));
    stream.flush();
    stream.close();
    Mockito.verify(mLocalOutputStream).flush();
  }
}
