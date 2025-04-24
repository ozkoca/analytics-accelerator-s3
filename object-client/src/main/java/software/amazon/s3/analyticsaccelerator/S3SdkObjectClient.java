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
package software.amazon.s3.analyticsaccelerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.request.*;

/** Object client, based on AWS SDK v2 */
public class S3SdkObjectClient implements ObjectClient {
  private static final String HEADER_REFERER = "Referer";
  private static final Logger LOG = LoggerFactory.getLogger(S3SdkObjectClient.class);

  private final S3Client s3Client;

  /**
   * Create an instance of a S3 client, with default configuration, for interaction with Amazon S3
   * compatible object stores. This takes ownership of the passed client and will close it on its
   * own close().
   *
   * @param s3Client Underlying client to be used for making requests to S3.
   */
  public S3SdkObjectClient(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  /** Closes the underlying client if instructed by the constructor. */
  @Override
  public void close() {}

  @Override
  public ObjectMetadata headObject(HeadRequest headRequest) {
    HeadObjectRequest.Builder builder =
        HeadObjectRequest.builder()
            .bucket(headRequest.getS3Uri().getBucket())
            .key(headRequest.getS3Uri().getKey());

    // Add User-Agent header to the request.
    builder.overrideConfiguration(AwsRequestOverrideConfiguration.builder().build());

    HeadObjectResponse headObjectResponse = s3Client.headObject(builder.build());
    return ObjectMetadata.builder()
        .contentLength(headObjectResponse.contentLength())
        .etag(headObjectResponse.eTag())
        .build();
  }

  @Override
  public ObjectContent getObject(GetRequest getRequest) {
    return getObject(getRequest, null);
  }

  @Override
  public ObjectContent getObject(GetRequest getRequest, StreamContext streamContext) {

    GetObjectRequest.Builder builder =
        GetObjectRequest.builder()
            .bucket(getRequest.getS3Uri().getBucket())
            .ifMatch(getRequest.getEtag())
            .key(getRequest.getS3Uri().getKey());

    final String range = getRequest.getRange().toHttpString();
    builder.range(range);

    final String referrerHeader;
    if (streamContext != null) {
      referrerHeader = streamContext.modifyAndBuildReferrerHeader(getRequest);
    } else {
      referrerHeader = getRequest.getReferrer().toString();
    }

    builder.overrideConfiguration(
        AwsRequestOverrideConfiguration.builder()
            .putHeader(HEADER_REFERER, referrerHeader)
            .build());

    LOG.info(
        "SDKObjectClient getObject STARTED uri: {}, range: {}, threadId: {}, threadName: {}, time: {}",
        getRequest.getS3Uri(),
        getRequest.getRange(),
        Thread.currentThread().getId(),
        Thread.currentThread().getName(),
        System.currentTimeMillis());

    ResponseInputStream<GetObjectResponse> responseInputStream =
        s3Client.getObject(builder.build());
    return ObjectContent.builder().stream(responseInputStream).build();
  }
}
