/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.property;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class InMemoryS3SeekableInputStream extends SeekableInputStream {

  private final SeekableInputStream delegate;

  /**
   * Returns an in memory S3 seekable stream. Pretends to point at s3://bucket/key and generates
   * `len` number of random bytes which will be then served back as object bytes.
   *
   * @param bucket the bucket
   * @param key the key
   * @param len the length of the data
   */
  public InMemoryS3SeekableInputStream(String bucket, String key, int len) throws IOException {
    S3URI s3URI = S3URI.of(bucket, key);
    ObjectClient objectClient = new InMemoryObjectClient(len);

    S3SeekableInputStreamFactory factory =
        new S3SeekableInputStreamFactory(objectClient, S3SeekableInputStreamConfiguration.DEFAULT);
    this.delegate = factory.createStream(s3URI);
  }

  private static class InMemoryObjectClient implements ObjectClient {
    private static final String etag = "Random";
    private final int size;
    private byte[] content;

    public InMemoryObjectClient(int size) {
      this.size = size;
      this.content = new byte[size];

      // Fill with random bytes
      ThreadLocalRandom.current().nextBytes(this.content);
    }

    @Override
    public ObjectMetadata headObject(HeadRequest headRequest) {
      return ObjectMetadata.builder().contentLength(size).etag(etag).build();
    }

    @Override
    public ObjectContent getObject(GetRequest getRequest) {
      return getObject(getRequest, null);
    }

    @Override
    public ObjectContent getObject(GetRequest getRequest, StreamContext streamContext) {
      int start = 0;
      int end = size - 1;

      if (Objects.nonNull(getRequest.getRange())) {
        start = (int) getRequest.getRange().getStart();
        end = (int) getRequest.getRange().getEnd();
      }

      byte[] range = Arrays.copyOfRange(this.content, start, end + 1);
      return ObjectContent.builder().stream(new ByteArrayInputStream(range)).build();
    }

    @Override
    public void close() throws IOException {
      this.content = null;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    this.delegate.seek(pos);
  }

  @Override
  public long getPos() {
    return this.delegate.getPos();
  }

  @Override
  public int readTail(byte[] buf, int off, int n) throws IOException {
    return this.delegate.readTail(buf, off, n);
  }

  @Override
  public void readFullyIntoBuffers(long position, List<ByteBuffer> byteBuffers) {}

  @Override
  public int read() throws IOException {
    return this.delegate.read();
  }

  @Override
  public void close() throws IOException {
    this.delegate.close();
  }
}
