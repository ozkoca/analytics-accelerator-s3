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
package software.amazon.s3.analyticsaccelerator.util;

import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.io.physical.LoggingUtil;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.Range;

/** Utility class for stream operations. */
public class StreamUtils {

  private static final int BUFFER_SIZE = 8 * ONE_KB;
  private static final Logger LOG = LoggerFactory.getLogger(StreamUtils.class);

  /**
   * Convert an InputStream from the underlying object to a byte array.
   *
   * @param objectContent the part of the object
   * @param objectKey container for S3 object to read
   * @param range range of the S3 object to read
   * @param timeoutMs read timeout in milliseconds
   * @return a byte array
   */
  public static byte[] toByteArray(
      ObjectContent objectContent, ObjectKey objectKey, Range range, long timeoutMs)
      throws IOException, TimeoutException {
    InputStream inStream = objectContent.getStream();
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[BUFFER_SIZE];
    try {
      int numBytesRead;
      long totalBytesRead = 0;
      long maxReadTime = 0;
      long maxReadBytes = 0;
      long minReadBytes = Long.MAX_VALUE;
      long totalReadTime = 0;
      long maxWriteTime = 0;
      long totalWriteTime = 0;
      int iteration = 0;
      int maxReadIteration = 0;
      int maxWriteIteration = 0;

      LoggingUtil.LogBuilder logger =
          LoggingUtil.start(LOG, "toByteArray: Starting to read from InputStream for Block")
              .withParam("s3Uri", objectKey.s3URI)
              .withParam("start", range.getStart())
              .withParam("end", range.getEnd())
              .withThreadInfo()
              .withTiming();
      logger.logStart();

      while (true) {
        iteration++;
        long readStart = System.currentTimeMillis();
        numBytesRead = inStream.read(buffer, 0, buffer.length);
        long readTime = System.currentTimeMillis() - readStart;

        if (readTime > maxReadTime) {
          maxReadTime = readTime;
          maxReadIteration = iteration;
        }

        totalReadTime += readTime;
        totalBytesRead += numBytesRead;
        maxReadBytes = Math.max(maxReadBytes, numBytesRead);
        minReadBytes = Math.min(minReadBytes, numBytesRead);

        if (numBytesRead == -1) break;

        long writeStart = System.currentTimeMillis();
        outStream.write(buffer, 0, numBytesRead);
        long writeTime = System.currentTimeMillis() - writeStart;

        if (writeTime > maxWriteTime) {
          maxWriteTime = writeTime;
          maxWriteIteration = iteration;
        }
        totalWriteTime += writeTime;
      }

      logger.logEnd();
      LOG.info(
          "StreamUtils toByteArray statistics: s3Uri: {}, start: {}, bytesRead: {}, maxReadTime: {}, maxReadBytes: {}, totalReadTime: {}, avgReadTime: {}, readThroughput: {}, maxWriteTime: {}, totalWriteTime: {}, avgWriteTime: {}, writeThroughput: {}, iteration: {}, maxReadIteration: {}, maxWriteIteration: {}, time: {}",
          objectKey.s3URI,
          range.getStart(),
          totalBytesRead,
          maxReadTime,
          maxReadBytes,
          totalReadTime,
          iteration == 0 ? 0 : (totalReadTime / (double) iteration),
          totalReadTime == 0 ? 0 : (totalBytesRead / (double) totalReadTime),
          maxWriteTime,
          totalWriteTime,
          iteration == 0 ? 0 : (totalWriteTime / (double) iteration),
          totalWriteTime == 0 ? 0 : (totalBytesRead / (double) totalWriteTime),
          iteration,
          maxReadIteration,
          maxWriteIteration,
          System.currentTimeMillis());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      inStream.close();
    }
    return outStream.toByteArray();
  }

  /**
   * sadsadsa
   *
   * @param buffers asd
   * @param objectContent dsa
   * @param objectKey dad
   * @param range sad
   * @throws IOException asds
   */
  public static void fillBuffers(
      List<ByteBuffer> buffers, ObjectContent objectContent, ObjectKey objectKey, Range range)
      throws IOException {
    long totalCapacity = buffers.stream().mapToLong(ByteBuffer::remaining).sum();
    long expectedSize = range.getEnd() - range.getStart() + 1;

    LOG.info(
        "fillBuffers- uri: {}, start: {}, end: {}, totalCapacity:{}, expectedSize: {}",
        objectKey.s3URI,
        range.getStart(),
        range.getEnd(),
        totalCapacity,
        expectedSize);
    LoggingUtil.LogBuilder logger =
        LoggingUtil.start(LOG, "fillBuffers:")
            .withParam("s3Uri", objectKey.s3URI)
            .withParam("start", range.getStart())
            .withParam("end", range.getEnd())
            .withThreadInfo()
            .withTiming();
    logger.logStart();
    try (InputStream inStream = objectContent.getStream()) {
      int bufferIndex = 0;

      while (bufferIndex < buffers.size()) {
        ByteBuffer currentBuffer = buffers.get(bufferIndex);

        if (!currentBuffer.hasRemaining()) {
          bufferIndex++;
          continue;
        }

        if (!currentBuffer.hasArray()) {
          throw new IllegalArgumentException("ByteBuffer does not have a backing array");
        }

        byte[] backingArray = currentBuffer.array();
        int position = currentBuffer.position();
        int arrayOffset = currentBuffer.arrayOffset() + position;
        int remaining = currentBuffer.remaining();

        int bytesToRead = Math.min(BUFFER_SIZE, remaining);

        int bytesRead = inStream.read(backingArray, arrayOffset, bytesToRead);

        if (bytesRead == -1) {
          break; // EOF
        }

        currentBuffer.position(position + bytesRead);
      }

      logger.logEnd();
    }
  }
}
