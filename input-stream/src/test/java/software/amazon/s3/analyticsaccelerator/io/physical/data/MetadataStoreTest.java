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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class MetadataStoreTest {

  @Test
  public void test__get__cacheWorks() throws IOException {
    // Given: a MetadataStore with caching turned on
    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata objectMetadata = ObjectMetadata.builder().etag("random").build();
    when(objectClient.headObject(any())).thenReturn(objectMetadata);
    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    S3URI key = S3URI.of("foo", "bar");

    // When: get(..) is called multiple times
    metadataStore.get(key);
    metadataStore.get(key);
    metadataStore.get(key);

    // Then: object store was accessed only once
    verify(objectClient, times(1)).headObject(any());
  }
}
