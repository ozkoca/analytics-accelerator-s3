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
package software.amazon.s3.analyticsaccelerator.request;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/** Represents APIs of an Amazon S3 compatible object store */
public interface ObjectClient extends Closeable {

  /**
   * Make a headObject request to the object store.
   *
   * @param headRequest The HEAD request to be sent
   * @param openStreamInformation contains stream information
   * @return an instance of {@link CompletableFuture} of type {@link ObjectMetadata}
   */
  CompletableFuture<ObjectMetadata> headObject(
      HeadRequest headRequest, OpenStreamInformation openStreamInformation);

  /**
   * Make a getObject request to the object store.
   *
   * @param getRequest The GET request to be sent
   * @param openStreamInformation contains stream information
   * @return an instance of {@link CompletableFuture} of type {@link ObjectContent}
   */
  CompletableFuture<ObjectContent> getObject(
      GetRequest getRequest, OpenStreamInformation openStreamInformation);
}
