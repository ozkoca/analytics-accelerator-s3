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
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.Referrer;
import software.amazon.s3.analyticsaccelerator.util.*;
import software.amazon.s3.analyticsaccelerator.util.retry.DefaultRetryStrategyImpl;
import software.amazon.s3.analyticsaccelerator.util.retry.RetryPolicy;
import software.amazon.s3.analyticsaccelerator.util.retry.RetryStrategy;

/**
 * A Block holding part of an object's data and owning its own async process for fetching part of
 * the object.
 */
public class Block implements Closeable {
  private CompletableFuture<ObjectContent> source;
  private CompletableFuture<byte[]> data;
  @Getter private final BlockKey blockKey;
  private final Telemetry telemetry;
  private final ObjectClient objectClient;
  private final OpenStreamInformation openStreamInformation;
  private final ReadMode readMode;
  private final Referrer referrer;
  private final long readTimeout;
  private final int readRetryCount;
  @Getter private final long generation;
  private final Metrics aggregatingMetrics;
  private final BlobStoreIndexCache indexCache;
  private static final String OPERATION_BLOCK_GET_ASYNC = "block.get.async";
  private static final String OPERATION_BLOCK_GET_JOIN = "block.get.join";
  private final RetryStrategy retryStrategy;

  private static final Logger LOG = LoggerFactory.getLogger(Block.class);

  /**
   * Constructs a Block data.
   *
   * @param blockKey the objectkey and range of the object
   * @param objectClient the object client to use to interact with the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   * @param readTimeout Timeout duration (in milliseconds) for reading a block object from S3
   * @param readRetryCount Number of retries for block read failure
   * @param aggregatingMetrics blobstore metrics
   * @param indexCache blobstore index cache
   * @param openStreamInformation contains stream information
   */
  public Block(
      @NonNull BlockKey blockKey,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      long generation,
      @NonNull ReadMode readMode,
      long readTimeout,
      int readRetryCount,
      @NonNull Metrics aggregatingMetrics,
      @NonNull BlobStoreIndexCache indexCache,
      @NonNull OpenStreamInformation openStreamInformation)
      throws IOException {

    long start = blockKey.getRange().getStart();
    long end = blockKey.getRange().getEnd();
    Preconditions.checkArgument(
        0 <= generation, "`generation` must be non-negative; was: %s", generation);
    Preconditions.checkArgument(0 <= start, "`start` must be non-negative; was: %s", start);
    Preconditions.checkArgument(0 <= end, "`end` must be non-negative; was: %s", end);
    Preconditions.checkArgument(
        start <= end, "`start` must be less than `end`; %s is not less than %s", start, end);
    Preconditions.checkArgument(
        0 < readTimeout, "`readTimeout` must be greater than 0; was %s", readTimeout);
    Preconditions.checkArgument(
        0 <= readRetryCount, "`readRetryCount` must be greater than -1; was %s", readRetryCount);

    this.generation = generation;
    this.telemetry = telemetry;
    this.blockKey = blockKey;
    this.objectClient = objectClient;
    this.openStreamInformation = openStreamInformation;
    this.readMode = readMode;
    this.referrer = new Referrer(this.blockKey.getRange().toHttpString(), readMode);
    this.readTimeout = readTimeout;
    this.readRetryCount = readRetryCount;
    this.aggregatingMetrics = aggregatingMetrics;
    this.indexCache = indexCache;
    this.retryStrategy = createRetryStrategy();
    this.generateSourceAndData();
  }

  /**
   * Helper to construct retryStrategy
   *
   * @return a {@link RetryStrategy} to retry when timeouts are set
   * @throws RuntimeException if all retries fails and an error occurs
   */
  private RetryStrategy createRetryStrategy() throws IOException {
    RetryStrategy base = new DefaultRetryStrategyImpl();
    RetryStrategy provided = this.openStreamInformation.getRetryStrategy();

    if (provided != null) {
      base = base.merge(provided);
    }
    if (this.readTimeout > 0) {
      RetryPolicy timeoutPolicy =
          RetryPolicy.builder()
              .handle(IOException.class, TimeoutException.class)
              .withMaxRetries(this.readRetryCount)
              .onRetry(this::generateSourceAndData)
              .build();
      base = base.amend(timeoutPolicy);
    }
    return base;
  }

  /**
   * Helper to construct source and data
   *
   * @throws IOException if all retries fails and an error occurs
   */
  private void generateSourceAndData() {
    GetRequest getRequest =
        GetRequest.builder()
            .s3Uri(this.blockKey.getObjectKey().getS3URI())
            .range(this.blockKey.getRange())
            .etag(this.blockKey.getObjectKey().getEtag())
            .referrer(referrer)
            .build();

    openStreamInformation.getRequestCallback().onGetRequest();

    this.source =
        this.telemetry.measureCritical(
            () ->
                Operation.builder()
                    .name(OPERATION_BLOCK_GET_ASYNC)
                    .attribute(StreamAttributes.uri(this.blockKey.getObjectKey().getS3URI()))
                    .attribute(StreamAttributes.etag(this.blockKey.getObjectKey().getEtag()))
                    .attribute(StreamAttributes.range(this.blockKey.getRange()))
                    .attribute(StreamAttributes.generation(generation))
                    .build(),
            () -> {
              this.aggregatingMetrics.add(MetricKey.GET_REQUEST_COUNT, 1);
              return objectClient.getObject(getRequest, openStreamInformation);
            });

    // Handle IOExceptions when converting stream to byte array
    this.data =
        this.source.thenApply(
            objectContent -> {
              try {
                byte[] bytes =
                    StreamUtils.toByteArray(
                        objectContent,
                        this.blockKey.getObjectKey(),
                        this.blockKey.getRange(),
                        this.readTimeout);
                int blockRange = blockKey.getRange().getLength();
                this.aggregatingMetrics.add(MetricKey.MEMORY_USAGE, blockRange);
                this.indexCache.put(blockKey, blockRange);
                return bytes;
              } catch (IOException | TimeoutException e) {
                throw new RuntimeException("Error while converting InputStream to byte array", e);
              }
            });
  }

  /** @return if data is loaded */
  public boolean isDataLoaded() {
    return data.isDone();
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException if an I/O error occurs
   */
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    byte[] content = this.retryStrategy.get(this::getData);
    indexCache.recordAccess(blockKey);
    return Byte.toUnsignedInt(content[posToOffset(pos)]);
  }

  /**
   * Reads data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   * @throws IOException if an I/O error occurs
   */
  public int read(byte @NonNull [] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    byte[] content = this.retryStrategy.get(this::getData);
    indexCache.recordAccess(blockKey);
    int contentOffset = posToOffset(pos);
    int available = content.length - contentOffset;
    int bytesToCopy = Math.min(len, available);

    for (int i = 0; i < bytesToCopy; ++i) {
      buf[off + i] = content[contentOffset + i];
    }

    return bytesToCopy;
  }

  /**
   * Does this block contain the position?
   *
   * @param pos the position
   * @return true if the byte at the position is contained by this block
   */
  public boolean contains(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    return this.blockKey.getRange().contains(pos);
  }

  /**
   * Determines the offset in the Block corresponding to a position in an object.
   *
   * @param pos the position of a byte in the object
   * @return the offset in the byte buffer underlying this Block
   */
  private int posToOffset(long pos) {
    return (int) (pos - this.blockKey.getRange().getStart());
  }

  /**
   * Returns the bytes fetched by the issued {@link GetRequest}. This method will block until the
   * data is fully available.
   *
   * @return the bytes fetched by the issued {@link GetRequest}.
   * @throws IOException if an I/O error occurs
   */
  private byte[] getData() throws IOException {
    return this.telemetry.measureJoinCritical(
        () ->
            Operation.builder()
                .name(OPERATION_BLOCK_GET_JOIN)
                .attribute(StreamAttributes.uri(this.blockKey.getObjectKey().getS3URI()))
                .attribute(StreamAttributes.etag(this.blockKey.getObjectKey().getEtag()))
                .attribute(StreamAttributes.range(this.blockKey.getRange()))
                .attribute(StreamAttributes.rangeLength(this.blockKey.getRange().getLength()))
                .build(),
        this.data,
        this.readTimeout);
  }

  /** Closes the {@link Block} and frees up all resources it holds */
  @Override
  public void close() {
    // Only the source needs to be canceled, the continuation will cancel on its own
    this.source.cancel(false);
  }
}
