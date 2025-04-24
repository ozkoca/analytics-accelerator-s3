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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = {"NP_NONNULL_PARAM_VIOLATION", "SIC_INNER_SHOULD_BE_STATIC_ANON"},
    justification =
        "We mean to pass nulls to checks. Also, closures cannot be made static in this case")
public class S3SdkObjectClientTest {

  private static final String HEADER_REFERER = "Referer";
  private static final S3URI TEST_URI = S3URI.of("test-bucket", "test-key");

  private static final String ETAG = "RandomString";
}
