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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.LoggingUtil;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.prefetcher.SequentialPatternDetector;
import software.amazon.s3.analyticsaccelerator.io.physical.prefetcher.SequentialReadProgression;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;

/** Implements a Block Manager responsible for planning and scheduling reads on a key. */
public class BlockManager implements Closeable {
  private final ObjectKey objectKey;
  private final ObjectMetadata metadata;
  private final BlockStore blockStore;
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final SequentialPatternDetector patternDetector;
  private final SequentialReadProgression sequentialReadProgression;
  private final IOPlanner ioPlanner;
  private final PhysicalIOConfiguration configuration;
  private final RangeOptimiser rangeOptimiser;
  private StreamContext streamContext;
  private final ExecutorService ioThreadPool;

  private static final String OPERATION_MAKE_RANGE_AVAILABLE = "block.manager.make.range.available";

  private static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);

  /**
   * Constructs a new BlockManager.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param metadata the metadata for the object we are reading
   * @param configuration the physicalIO configuration
   * @param ioThreadPool thread pool
   */
  public BlockManager(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectClient objectClient,
      @NonNull ObjectMetadata metadata,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration,
      @NonNull ExecutorService ioThreadPool) {
    this(objectKey, objectClient, metadata, telemetry, configuration, null, ioThreadPool);
  }

  /**
   * Constructs a new BlockManager.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param metadata the metadata for the object
   * @param configuration the physicalIO configuration
   * @param streamContext contains audit headers to be attached in the request header
   * @param ioThreadPool thread pool
   */
  public BlockManager(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectClient objectClient,
      @NonNull ObjectMetadata metadata,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration,
      StreamContext streamContext,
      @NonNull ExecutorService ioThreadPool) {
    this.objectKey = objectKey;
    this.objectClient = objectClient;
    this.metadata = metadata;
    this.telemetry = telemetry;
    this.configuration = configuration;
    this.blockStore = new BlockStore(objectKey, metadata);
    this.patternDetector = new SequentialPatternDetector(blockStore);
    this.sequentialReadProgression = new SequentialReadProgression(configuration);
    this.ioPlanner = new IOPlanner(blockStore);
    this.rangeOptimiser = new RangeOptimiser(configuration);
    this.streamContext = streamContext;
    this.ioThreadPool = ioThreadPool;
  }

  /**
   * Given the position of a byte, return the block holding it.
   *
   * @param pos the position of a byte
   * @return the Block holding the byte or empty if the byte is not in the BlockStore
   */
  public synchronized Optional<Block> getBlock(long pos) {
    return this.blockStore.getBlock(pos);
  }

  /**
   * Make sure that the byte at a give position is in the BlockStore.
   *
   * @param pos the position of the byte
   * @param readMode whether this ask corresponds to a sync or async read
   * @throws IOException if an I/O error occurs
   */
  public synchronized void makePositionAvailable(long pos, ReadMode readMode) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    // Position is already available --> return corresponding block
    if (getBlock(pos).isPresent()) {
      return;
    }

    makeRangeAvailable(pos, 1, readMode);
  }

  private boolean isRangeAvailable(long pos, long len) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");

    long lastByteOfRange = pos + len - 1;

    OptionalLong nextMissingByte = blockStore.findNextMissingByte(pos);
    if (nextMissingByte.isPresent()) {
      return lastByteOfRange < nextMissingByte.getAsLong();
    }

    // If there is no missing byte after pos, then the whole object is already fetched
    return true;
  }

  /**
   * Method that ensures that a range is fully available in the object store. After calling this
   * method the BlockStore should contain all bytes in the range and we should be able to service a
   * read through the BlockStore.
   *
   * @param pos start of a read
   * @param len length of the read
   * @param readMode whether this ask corresponds to a sync or async read
   * @throws IOException if an I/O error occurs
   */
  public synchronized void makeRangeAvailable(long pos, long len, ReadMode readMode)
      throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");

    if (isRangeAvailable(pos, len)) {
      return;
    }

    LoggingUtil.LogBuilder logger =
        LoggingUtil.start(LOG, "makeRangeAvailable")
            .withParam("S3URI", this.objectKey.getS3URI())
            .withParam("pos", pos)
            .withParam("len", len)
            .withParam("readMode", readMode)
            .withThreadInfo()
            .withTiming();
    logger.logStart();

    // In case of a sequential reading pattern, calculate the generation and adjust the requested
    // effectiveEnd of the requested range
    long effectiveEnd = pos + Math.max(len, configuration.getReadAheadBytes()) - 1;

    // Check sequential prefetching. If read mode is ASYNC, that is the request is from the parquet
    // prefetch path, then do not extend the request.
    // TODO: Improve readModes, as tracked in
    // https://github.com/awslabs/analytics-accelerator-s3/issues/195
    final long generation = 0;
    //    if (readMode != ReadMode.ASYNC && patternDetector.isSequentialRead(pos)) {
    //      generation = patternDetector.getGeneration(pos);
    //      effectiveEnd =
    //          Math.max(
    //              effectiveEnd,
    //              truncatePos(pos + sequentialReadProgression.getSizeForGeneration(generation)));
    //    } else {
    //      generation = 0;
    //    }

    Range range = new Range(pos, Math.min(effectiveEnd, getLastObjectByte()));
    Block block =
        new Block(
            objectKey,
            objectClient,
            telemetry,
            range.getStart(),
            range.getEnd(),
            generation,
            readMode,
            this.configuration.getBlockReadTimeout(),
            this.configuration.getBlockReadRetryCount(),
            streamContext,
            ioThreadPool);
    blockStore.add(block);

    logger.logEnd();
  }

  private long getLastObjectByte() {
    return this.metadata.getContentLength() - 1;
  }

  /** Closes the {@link BlockManager} and frees up all resources it holds */
  @Override
  public void close() {
    blockStore.close();
  }
}
