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
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler implements PartitionLevelConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);

  private long _lastOffsetReceived = -1;
  protected Reader<byte[]> _pulsarReader = null;

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

  private void createReader (MessageId messageId) throws PulsarClientException {
    LOGGER.info("Creating reader with start messageId: {}", messageId);
    String topic = _topic + "-partition-" + _partition;
    _pulsarReader = _pulsarClient
      .newReader()
      .topic(topic)
      .startMessageId(messageId)
      .create();
  }

  public MessageBatch<byte[]> fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws TimeoutException {
    LOGGER.info("fetchMessages() called, startOffset: {}, endOffset: {}, timeoutMillis: {}, _lastOffsetReceived: {}", startOffset, endOffset, timeoutMillis, _lastOffsetReceived); // TODO: move this to debug level

    if (_pulsarReader == null || (startOffset <= _lastOffsetReceived && _lastOffsetReceived != -1)) {// TODO: we could seek() + add comment
      if (_pulsarReader != null) {
        LOGGER.warn("Trying to read messages already read from pulsar with a startOffset ({}) <= lastOffsetReceived ({})", startOffset, _lastOffsetReceived);
        try {
          _pulsarReader.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close reader", e);
        }
      }
      try {
        createReader(startOffset == -1 ? MessageId.earliest : MessageIdUtils.getMessageId(startOffset, _partition));
      } catch (PulsarClientException e) {
        LOGGER.error("Could not create reader for pulsar", e);
        throw new RuntimeException("Could not create reader", e);
      }
    }
    // if (_pulsarReader == null) {
    //   try {
    //     createReader(MessageIdUtils.getMessageId(startOffset, _partition));
    //   } catch (PulsarClientException e) {
    //     LOGGER.error("Could not create reader for pulsar", e);
    //     throw new RuntimeException("Could not create reader", e);
    //   }
    // }

    if (endOffset != Long.MAX_VALUE) {
      LOGGER.error("Could not read messages from pulsar with an endOffset");
      return new PulsarMessageBatch(Collections.emptyList());
    }

    // if (startOffset <= _lastOffsetReceived) {
    //   try {
    //     _pulsarReader.seek(MessageIdUtils.getMessageId(startOffset, _partition));
    //   } catch (PulsarClientException e) {
    //     LOGGER.error("Could not seek to {} reader for pulsar", startOffset, e);
    //     throw new RuntimeException("Could not update cursor", e);
    //   }
    // }

    List<MessageAndOffset> messages = new ArrayList<>();
    try {
      while (_pulsarReader.hasMessageAvailable()) {
        Message<byte[]> record = _pulsarReader.readNext();
        long offset = MessageIdUtils.getOffset(record.getMessageId());
        LOGGER.info("Readed message with id {}, offset {}", record.getMessageId(), offset); // TODO: move this to debug level
        messages.add(new MessageAndOffset(record.getData(), offset));

        if (messages.size() >= _config.getMaximumBatchMessagesCount()) {
          break; // we read enough message for this iteration
        }
      }
    } catch (PulsarClientException e) {
      LOGGER.error("Could not read messages from pulsar", e);
      return new PulsarMessageBatch(Collections.emptyList());
    }
    LOGGER.info("Found {} messages", messages.size()); // TODO: move this to debug level
    if (messages.size() > 0) {
      _lastOffsetReceived = messages.get(messages.size() - 1).getOffset();
    }
    return new PulsarMessageBatch(messages);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    if (_pulsarReader != null) {
      _pulsarReader.close();
    } 
  }
}
