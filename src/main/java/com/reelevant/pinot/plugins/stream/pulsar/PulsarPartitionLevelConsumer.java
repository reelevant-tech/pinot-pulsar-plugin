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

import com.google.common.collect.Iterables;
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
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler implements PartitionLevelConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);

  private long _lastOffsetReceived = -1;
  private boolean hasSeek = false;

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) throws IOException {
    super(clientId, streamConfig, partition);
  }

  @Override
  public MessageBatch<byte[]> fetchMessages(StreamPartitionMsgOffset startMsgOffset, StreamPartitionMsgOffset endMsgOffset,
      int timeoutMillis)
      throws TimeoutException {
    final long startOffset = ((LongMsgOffset)startMsgOffset).getOffset();
    final long endOffset = endMsgOffset == null ? Long.MAX_VALUE : ((LongMsgOffset)endMsgOffset).getOffset();
    return fetchMessages(startOffset, endOffset, timeoutMillis);
  }

  public MessageBatch<byte[]> fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws TimeoutException {
    Messages<byte[]> batchMessages;
    try {
      // if the new offset is above the old one, that means we succesfully ingested the previous batch
      if (startOffset > _lastOffsetReceived && _lastOffsetReceived != -1) {
        _pulsarConsumer.acknowledgeCumulative(MessageIdUtils.getMessageId(_lastOffsetReceived, _partition));
      }
      if (hasSeek == false) {
        LOGGER.info("Seeking to offset {}", startOffset);
        hasSeek = true;
        _pulsarConsumer.seek(MessageIdUtils.getMessageId(startOffset, _partition));
        LOGGER.info("Seeking to offset {} finished.", startOffset);
      }
      batchMessages = _pulsarConsumer.batchReceive();
      // avoid overhead when there are no messages
      if (batchMessages.size() == 0) {
        return new PulsarMessageBatch(Collections.emptyList());
      }
    } catch (PulsarClientException e) {
      // we 
      LOGGER.error("Could not read messages from pulsar", e);
      return new PulsarMessageBatch(Collections.emptyList());
    }
    // we receive a batch of message and we are filtering out messages with offset above endOffset
    // however endOffset is generally Long.MAX_VALUE
    Iterable<Message<byte[]>> filteredMessages =
        buildOffsetFilteringIterable(batchMessages, startOffset, endOffset);
    // construct a messageBatch for pulsar
    List<MessageAndOffset> messages = new ArrayList<>();
    for (Message<byte[]> record : filteredMessages) {
      messages.add(new MessageAndOffset(record.getData(), MessageIdUtils.getOffset(record.getMessageId())));
    }
    PulsarMessageBatch batch = new PulsarMessageBatch(messages);
    _lastOffsetReceived = messages.get(messages.size() - 1).getOffset();
    return batch;
  }

  private Iterable<Message<byte[]>> buildOffsetFilteringIterable(
      Messages<byte[]> messages, final long startOffset, final long endOffset) {
    return Iterables.filter(messages, input -> {
      long offset = MessageIdUtils.getOffset(input.getMessageId());
      // Filter messages that are either null or have an offset ∉ [startOffset, endOffset]
      return input != null && offset >= startOffset && (endOffset > offset || endOffset == -1);
    });
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
