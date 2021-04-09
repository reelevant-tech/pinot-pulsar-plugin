/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarStreamMetadataProvider extends PulsarPartitionLevelConnectionHandler implements StreamMetadataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarStreamMetadataProvider.class);

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig) throws IOException {
    this(clientId, streamConfig, Integer.MIN_VALUE);
  }

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) throws IOException {
    super(clientId, streamConfig, partition);
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      return _pulsarClient.getPartitionsForTopic(_topic).get(15, TimeUnit.SECONDS).size();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOGGER.error("Could noot retrieve parititon for topic " + _topic, e);
      return 0;
    }
  }

  public synchronized long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws java.util.concurrent.TimeoutException {
    throw new UnsupportedOperationException("The use of this method is not supported");
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws TimeoutException {
    Preconditions.checkNotNull(offsetCriteria);
    long offset = -1;
    if (offsetCriteria.isLargest()) {
      offset = MessageIdUtils.getOffset(MessageId.latest);
    } else if (offsetCriteria.isSmallest()) {
      offset = MessageIdUtils.getOffset(MessageId.earliest);
    } else {
      throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria.toString());
    }
    return new LongMsgOffset(offset);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
