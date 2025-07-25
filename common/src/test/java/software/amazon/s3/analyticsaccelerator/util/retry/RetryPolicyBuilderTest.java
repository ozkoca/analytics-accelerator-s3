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
package software.amazon.s3.analyticsaccelerator.util.retry;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RetryPolicyBuilderTest {

  @Test
  void testBuildCreatesRetryPolicy() {
    RetryPolicyBuilder builder = new RetryPolicyBuilder();
    RetryPolicy policy = builder.build();

    assertNotNull(policy);
    assertNotNull(policy.getDelegate());
  }

  @Test
  void testWithMaxRetries() {
    RetryPolicyBuilder builder = new RetryPolicyBuilder();
    RetryPolicyBuilder result = builder.withMaxRetries(5);

    assertSame(builder, result);
    assertNotNull(builder.build());
  }

  @Test
  void testWithDelay() {
    RetryPolicyBuilder builder = new RetryPolicyBuilder();
    Duration delay = Duration.ofSeconds(1);
    RetryPolicyBuilder result = builder.withDelay(delay);

    assertSame(builder, result);
    assertNotNull(builder.build());
  }

  @Test
  void testHandleSingleException() {
    RetryPolicyBuilder builder = new RetryPolicyBuilder();
    RetryPolicyBuilder result = builder.handle(IOException.class);

    assertSame(builder, result);
    assertNotNull(builder.build());
  }

  @Test
  void testHandleMultipleExceptions() {
    RetryPolicyBuilder builder = new RetryPolicyBuilder();
    RetryPolicyBuilder result = builder.handle(IOException.class, RuntimeException.class);

    assertSame(builder, result);
    assertNotNull(builder.build());
  }

  @Test
  void testChainedConfiguration() {
    RetryPolicy policy =
        new RetryPolicyBuilder()
            .withMaxRetries(3)
            .withDelay(Duration.ofMillis(500))
            .handle(IOException.class)
            .build();

    assertNotNull(policy);
    assertNotNull(policy.getDelegate());
  }
}
