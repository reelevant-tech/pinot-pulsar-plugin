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
package com.reelevant.pinot.plugins.stream.pulsar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler implements PartitionLevelConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);

  private long _lastOffsetReceived = -1;

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) throws IOException {
    super(clientId, streamConfig, partition);
    LOGGER.info("Constructed new PulsarPartitionLevelConsumer, clientId: {}, streamConfig: {}, partition: {}", clientId, streamConfig, partition);
  }

  @Override
  public MessageBatch<byte[]> fetchMessages(StreamPartitionMsgOffset startMsgOffset, StreamPartitionMsgOffset endMsgOffset,
      int timeoutMillis)
      throws TimeoutException {
    LOGGER.debug("fetchMessages() called, startMsgOffset: {}, endMsgOffset: {}, timeoutMillis: {}", startMsgOffset, endMsgOffset, timeoutMillis);
    final long startOffset = ((LongMsgOffset)startMsgOffset).getOffset();
    final long endOffset = endMsgOffset == null ? Long.MAX_VALUE : ((LongMsgOffset)endMsgOffset).getOffset();
    return fetchMessages(startOffset, endOffset, timeoutMillis);
  }

  public MessageBatch<byte[]> fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws TimeoutException {
    LOGGER.info("fetchMessages() called, startOffset: {}, endOffset: {}, timeoutMillis: {}", startOffset, endOffset, timeoutMillis); // TODO: move this to debug level

    if (endOffset != Long.MAX_VALUE) {
      LOGGER.error("Could not read messages from pulsar with an endOffset");
      return new PulsarMessageBatch(Collections.emptyList());
    }

    if (startOffset != -1 && startOffset < _lastOffsetReceived) {
      LOGGER.error("Could not read messages from pulsar with a startOffset ({}) < lastOffsetReceived ({})", startOffset, _lastOffsetReceived);
      return new PulsarMessageBatch(Collections.emptyList());
    }

    List<MessageAndOffset> messages = new ArrayList<>();
    try {
      while (_pulsarReader.hasMessageAvailable()) {
        Message<byte[]> record = _pulsarReader.readNext();
        LOGGER.info("Readed message with id {}", record.getMessageId()); // TODO: move this to debug level
        messages.add(new MessageAndOffset(record.getData(), MessageIdUtils.getOffset(record.getMessageId())));
      }
    } catch (PulsarClientException e) {
      LOGGER.error("Could not read messages from pulsar", e);
      return new PulsarMessageBatch(Collections.emptyList());
    }
    LOGGER.info("Found {} messages", messages.size()); // TODO: move this to debug level
    _lastOffsetReceived = messages.get(messages.size() - 1).getOffset();
    return new PulsarMessageBatch(messages);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
