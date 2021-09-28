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
// import java.util.HashMap;
import java.util.List;
// import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler implements PartitionLevelConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);

  private long _lastOffsetReceived = -1;
  protected Consumer<byte[]> _pulsarConsumer = null;
  private boolean consumerInited = false;

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) throws IOException {
    super(clientId, streamConfig, partition);
    LOGGER.info("Construct new PulsarPartitionLevelConsumer, clientId: {}, streamConfig: {}, partition: {}", clientId, streamConfig, partition);

    // this class is only instancied by the pinot-server
    String topic = _topic + "-partition-" + _partition;
    String subscription = "pinot-reader-" + UUID.randomUUID().toString().substring(0, 10);
    _pulsarConsumer = _pulsarClient
      .newConsumer()
      .topic(topic)
      .subscriptionName(subscription)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .subscriptionType(SubscriptionType.Exclusive)
      .subscriptionMode(SubscriptionMode.NonDurable)
      .enableBatchIndexAcknowledgment(false)
      .batchReceivePolicy(BatchReceivePolicy.builder()
        .maxNumMessages(_config.getMaximumBatchMessagesCount())
        .maxNumBytes(_config.getMaximumBatchSize())
        .timeout(_config.getBatchTimeout(), TimeUnit.MILLISECONDS)
        .build())
      .subscribe();
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
    MessageId startMessageId = MessageIdUtils.getMessageId(startOffset, _partition);
    MessageId lastMessageId = MessageIdUtils.getMessageId(_lastOffsetReceived, _partition);
    LOGGER.debug("fetchMessages() called, startOffset: {}, startMessageId: {}, endOffset: {}, timeoutMillis: {}, _lastOffsetReceived: {}, lastMessageId: {}", startOffset, startMessageId, endOffset, timeoutMillis, _lastOffsetReceived, lastMessageId); 

    // we don't handle endOffsets
    if (endOffset != Long.MAX_VALUE) {
      LOGGER.error("Could not read messages from pulsar with an endOffset");
      return new PulsarMessageBatch(Collections.emptyList());
    }
    
    /**
     * 1. Launch read with startOffset=-1
     * 2. We seek to the earliest
     * 3. Read messages
     * 4. Launch read with startOffset= lastMessage.offset + 1 (cf. MessageAndOffset.java)
     * 5. If no anymore messages, Pinot restart consumer (by default, after 35 iterations it restart)
     * 6. Launch read with startOffset= lastMessage.offset + 1
     * 
     * If startOffset<lastMessage.offset+1, we want to read previous messages, we need to seek
     * If startOffset>lastMessage.offset+1, we want to read newest messages, we need to seek
     */

    if (
      // if the consumer hasn't been inited yet, we need to seek() to set the cursor
      // to the right position
      consumerInited == false ||
      // if pinot is trying to read an offset that we've already read/ack with this consumer
      // we need to seek() too to move the cursor
      // note: we need to check if _lastOffsetReceived != -1 because
      // we could have inited the consumer but not read any messages yet
      (_lastOffsetReceived != -1 && startOffset != _lastOffsetReceived + 1)
    ) {
      consumerInited = true;
      MessageId messageId = startOffset == -1 ? MessageId.earliest : MessageIdUtils.getMessageId(startOffset, _partition);
      LOGGER.info("Seeking reader with messageId: {}, startOffset: {}, _lastOffsetReceived: {}", messageId, startOffset, _lastOffsetReceived);
      try {
        _pulsarConsumer.seek(messageId);
      } catch (PulsarClientException e) {
        LOGGER.error("Could not seek reader for pulsar", e);
        throw new RuntimeException("Could not seek reader", e);
      }
    }

    List<MessageAndOffset> messages = new ArrayList<>();
    try {
      // read from pulsar
      Messages<byte[]> batchMessages = _pulsarConsumer.batchReceive();
      LOGGER.debug("Found {} messages", batchMessages.size());
      if (batchMessages.size() == 0) { // stop here with no messages, and avoid any exception when trying to set lastOffsetReceived
        return new PulsarMessageBatch(Collections.emptyList());
      }

      // format for pinot
      for (Message<byte[]> record : batchMessages) {
        long offset = MessageIdUtils.getOffset(record.getMessageId());
        LOGGER.debug("Read message with id {}, offset {}", record.getMessageId(), offset);
        messages.add(new MessageAndOffset(record.getData(), offset));
      }
    } catch (PulsarClientException e) {
      LOGGER.error("Could not read messages from pulsar", e);
      return new PulsarMessageBatch(Collections.emptyList());
    }

    // save state
    _lastOffsetReceived = messages.get(messages.size() - 1).getOffset();
    MessageId messageId = MessageIdUtils.getMessageId(_lastOffsetReceived, _partition);
    try {
      // we ack messages here because anyway it's pinot that choose the cursor
      LOGGER.debug("Ack messages until {} offset, messageId {}", _lastOffsetReceived, messageId);
      _pulsarConsumer.acknowledgeCumulative(messageId);
    } catch (PulsarClientException e) {
      LOGGER.error("Failed to ack messages until {}", messageId, e);
    }

    // give messages to pinot
    return new PulsarMessageBatch(messages);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    if (_pulsarConsumer != null) {
      _pulsarConsumer.close();
    } 
  }
}
