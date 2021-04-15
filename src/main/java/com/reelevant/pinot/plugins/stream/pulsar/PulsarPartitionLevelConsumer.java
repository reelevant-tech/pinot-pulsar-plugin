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

  /**
   * We can't use a Reader because we're not able to set
   * `.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)`
   * which seems needed when pinot re-create the reader (because it's idle)
   * see https://github.com/apache/pulsar/blob/6704f12104219611164aa2bb5bbdfc929613f1bf/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ReaderImpl.java#L52
   * We can't use a Consumer because we can't set `startMessageId = messageId`
   * see https://github.com/apache/pulsar/blob/6704f12104219611164aa2bb5bbdfc929613f1bf/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java#L235
   */
  private void createReader (long offset) throws PulsarClientException {
    // note: we need to substract 1 to the offset if we're not trying to read from the beginning.
    // because Pinot send us the lastOffset we receive + 1 (see MessageAndOffset.java)
    // so we need to seek() at the previous offset since it's exclusive
    MessageId messageId = offset == -1 ? MessageId.earliest : MessageIdUtils.getMessageId(offset - 1, _partition);
    LOGGER.info("Creating reader with start messageId: {}", messageId);
    String topic = _topic + "-partition-" + _partition;
    String subscription = "pinot-reader-" + UUID.randomUUID().toString().substring(0, 10);

    // Map<String, Object> conf = new HashMap<String, Object>();
    // conf.put("subscriptionInitialPosition", SubscriptionInitialPosition.Earliest);

    _pulsarConsumer = _pulsarClient
      .newConsumer()
      // .loadConf(conf)
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
    _pulsarConsumer.seek(messageId);
  }

  public MessageBatch<byte[]> fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws TimeoutException {
    MessageId startMessageId = MessageIdUtils.getMessageId(startOffset, _partition);
    MessageId lastMessageId = MessageIdUtils.getMessageId(_lastOffsetReceived, _partition);
    LOGGER.info("fetchMessages() called, startOffset: {}, startMessageId: {}, endOffset: {}, timeoutMillis: {}, _lastOffsetReceived: {}, lastMessageId: {}", startOffset, startMessageId, endOffset, timeoutMillis, _lastOffsetReceived, lastMessageId); // TODO: move this to debug level

    if (_pulsarConsumer == null || (startOffset <= _lastOffsetReceived && _lastOffsetReceived != -1)) {// TODO: we could seek() + add comment
      if (_pulsarConsumer != null) {
        LOGGER.warn("Trying to read messages already read from pulsar with a startOffset ({}) <= lastOffsetReceived ({})", startOffset, _lastOffsetReceived);
        try {
          _pulsarConsumer.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close reader", e);
        }
      }
      try {
        createReader(startOffset);
      } catch (PulsarClientException e) {
        LOGGER.error("Could not create reader for pulsar", e);
        throw new RuntimeException("Could not create reader", e);
      }
    }

    // we don't handle endOffsets
    if (endOffset != Long.MAX_VALUE) {
      LOGGER.error("Could not read messages from pulsar with an endOffset");
      return new PulsarMessageBatch(Collections.emptyList());
    }

    try {
      Thread.sleep(3000); // ca marche bien avec le premier consumer, ca fuckup apres
      /*
      pinot-server_1      | 2021/04/15 10:27:03.582 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354564, startMessageId: 10:4:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354563, lastMessageId: 10:3:0
      */
      /**
ca fonctionne bien avec le premier consumer:
pinot-server_1      | 2021/04/15 10:25:43.614 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 1 messages
pinot-server_1      | 2021/04/15 10:25:43.614 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Read message with id 10:2:0, offset 2684354562
pinot-server_1      | 2021/04/15 10:25:43.614 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Ack messages until 2684354562 offset, messageId 10:2:0
pinot-server_1      | 2021/04/15 10:25:43.616 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354563, startMessageId: 10:3:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354562, lastMessageId: 10:2:0


bien seek:
pinot-server_1      | 2021/04/15 10:28:07.713 INFO [ConsumerImpl] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] [persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0][pinot-reader-06f7385a-5] Seek subscription to message id 10:4:0

pourtant aucun message de lu:
--> est-ce que pr lui le message 5 c'est pas le 0??? vu que c'est une nouvelle subscription, son counter est a 0?
-----> on dirait que non, psk ca a remarcher aprs

--> quand le consumer est recree, le premiere message send est perdu, mais des qu'un deuxeieme message
est envoye, ca le debloque, il trouve le deuxieme et les suivants
et ainsi de suite a chaque re-creation
par contre j'ai bien l'impression que le premier consumer n'a pas de soucis avec lire le premier message (ptet psk c le premier du topic aussi??)
inot-server_1      | 2021/04/15 10:28:49.340 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354564, startMessageId: 10:4:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pinot-server_1      | 2021/04/15 10:28:52.443 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:28:52.544 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354564, startMessageId: 10:4:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pulsar_1            | 10:28:53.749 [pulsar-web-68-1] INFO  org.eclipse.jetty.server.RequestLog - 127.0.0.1 - - [15/Apr/2021:10:28:53 +0000] "GET /admin/v2/persistent/public/functions/coordinate/stats?getPreciseBacklog=false&subscriptionBacklogSize=false HTTP/1.1" 200 1677 "-" "Pulsar-Java-v2.7.1" 12
pinot-server_1      | 2021/04/15 10:28:55.647 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:28:55.747 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354564, startMessageId: 10:4:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pulsar_1            | 10:28:57.887 [pulsar-io-50-4] INFO  org.apache.pulsar.broker.service.ServerCnx - New connection from /192.168.32.1:64574
pulsar_1            | 10:28:57.895 [pulsar-io-50-4] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64574][persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0] Creating producer. producerId=0
pulsar_1            | 10:28:57.897 [ForkJoinPool.commonPool-worker-3] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64574] persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0 configured with schema false
pulsar_1            | 10:28:57.897 [ForkJoinPool.commonPool-worker-3] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64574] Created new producer: Producer{topic=PersistentTopic{topic=persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0}, client=/192.168.32.1:64574, producerName=standalone-0-4, producerId=0}
pinot-server_1      | 2021/04/15 10:28:58.851 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:28:58.951 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354564, startMessageId: 10:4:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pinot-server_1      | 2021/04/15 10:29:02.054 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:29:02.155 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354564, startMessageId: 10:4:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pinot-server_1      | 2021/04/15 10:29:05.258 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:29:05.359 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354564, startMessageId: 10:4:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0



pinot-server_1      | 2021/04/15 10:35:48.047 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:35:48.147 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pulsar_1            | 10:35:49.435 [pulsar-io-50-6] INFO  org.apache.pulsar.broker.service.ServerCnx - New connection from /192.168.32.1:64626
pulsar_1            | 10:35:49.448 [pulsar-io-50-6] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64626][persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0] Creating producer. producerId=0
pulsar_1            | 10:35:49.451 [ForkJoinPool.commonPool-worker-0] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64626] persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0 configured with schema false
pulsar_1            | 10:35:49.452 [ForkJoinPool.commonPool-worker-0] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64626] Created new producer: Producer{topic=PersistentTopic{topic=persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0}, client=/192.168.32.1:64626, producerName=standalone-0-10, producerId=0}
pinot-server_1      | 2021/04/15 10:35:51.249 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:35:51.350 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pulsar_1            | 10:35:53.272 [pulsar-web-68-4] INFO  org.eclipse.jetty.server.RequestLog - 127.0.0.1 - - [15/Apr/2021:10:35:53 +0000] "GET /admin/v2/persistent/public/functions/coordinate/stats?getPreciseBacklog=false&subscriptionBacklogSize=false HTTP/1.1" 200 1677 "-" "Pulsar-Java-v2.7.1" 9
pinot-server_1      | 2021/04/15 10:35:54.452 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:35:54.553 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pinot-server_1      | 2021/04/15 10:35:57.655 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:35:57.755 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pinot-server_1      | 2021/04/15 10:36:00.858 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:00.958 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pinot-server_1      | 2021/04/15 10:36:04.061 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:04.162 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pinot-broker_1      | Failed to find time boundary info for hybrid table: datasource_6078145ea64b0003002b3292
pinot-broker_1      | Failed to find time boundary info for hybrid table: datasource_6078145ea64b0003002b3292
pinot-broker_1      | requestId=14,table=datasource_6078145ea64b0003002b3292,timeMs=13,docs=9/9,entries=0/288,segments(queried/processed/matched/consuming/unavailable):1/1/1/1/0,consumingFreshnessTimeMs=1618482800897,servers=1/1,groupLimitReached=false,brokerReduceTimeMs=2,exceptions=0,serverStats=(Server=SubmitDelayMs,ResponseDelayMs,ResponseSize,DeserializationTimeMs);192.168.32.8_R=1,8,3678,0,query=select * from datasource_6078145ea64b0003002b3292 limit 10
pinot-server_1      | 2021/04/15 10:36:07.229 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:07.330 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pinot-server_1      | 2021/04/15 10:36:10.432 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:10.532 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pulsar_1            | 10:36:12.752 [pulsar-io-50-6] INFO  org.apache.pulsar.broker.service.ServerCnx - Closed connection from /192.168.32.1:64626
pinot-server_1      | 2021/04/15 10:36:13.635 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:13.736 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pinot-server_1      | 2021/04/15 10:36:16.838 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:16.939 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354570, startMessageId: 10:10:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: -1, lastMessageId: 68719476735:268435455:0
pulsar_1            | 10:36:17.981 [pulsar-io-50-7] INFO  org.apache.pulsar.broker.service.ServerCnx - New connection from /192.168.32.1:64632
pulsar_1            | 10:36:17.988 [pulsar-io-50-7] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64632][persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0] Creating producer. producerId=0
pulsar_1            | 10:36:17.991 [ForkJoinPool.commonPool-worker-2] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64632] persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0 configured with schema false
pulsar_1            | 10:36:17.992 [ForkJoinPool.commonPool-worker-2] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64632] Created new producer: Producer{topic=PersistentTopic{topic=persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0}, client=/192.168.32.1:64632, producerName=standalone-0-11, producerId=0}
pinot-server_1      | 2021/04/15 10:36:20.042 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 1 messages
pinot-server_1      | 2021/04/15 10:36:20.042 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Read message with id 10:11:0, offset 2684354571
pinot-server_1      | 2021/04/15 10:36:20.042 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Ack messages until 2684354571 offset, messageId 10:11:0
pinot-server_1      | 2021/04/15 10:36:20.043 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pinot-server_1      | 2021/04/15 10:36:23.146 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pulsar_1            | 10:36:23.230 [pulsar-web-68-8] INFO  org.eclipse.jetty.server.RequestLog - 127.0.0.1 - - [15/Apr/2021:10:36:23 +0000] "GET /admin/v2/persistent/public/functions/coordinate/stats?getPreciseBacklog=false&subscriptionBacklogSize=false HTTP/1.1" 200 1677 "-" "Pulsar-Java-v2.7.1" 3
pinot-server_1      | 2021/04/15 10:36:23.247 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pinot-server_1      | 2021/04/15 10:36:26.349 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:26.450 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pinot-broker_1      | Failed to find time boundary info for hybrid table: datasource_6078145ea64b0003002b3292
pinot-broker_1      | Failed to find time boundary info for hybrid table: datasource_6078145ea64b0003002b3292
pinot-broker_1      | requestId=15,table=datasource_6078145ea64b0003002b3292,timeMs=23,docs=10/10,entries=0/320,segments(queried/processed/matched/consuming/unavailable):1/1/1/1/0,consumingFreshnessTimeMs=1618482980043,servers=1/1,groupLimitReached=false,brokerReduceTimeMs=3,exceptions=0,serverStats=(Server=SubmitDelayMs,ResponseDelayMs,ResponseSize,DeserializationTimeMs);192.168.32.8_R=0,18,3828,0,query=select * from datasource_6078145ea64b0003002b3292 limit 10
pinot-server_1      | 2021/04/15 10:36:29.552 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:29.653 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pinot-server_1      | 2021/04/15 10:36:32.756 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:32.858 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pinot-server_1      | 2021/04/15 10:36:35.925 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:36.025 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pinot-server_1      | 2021/04/15 10:36:39.127 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:39.227 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pinot-proxy_1       | {"level":"info","msg":"List of brokers successfuly updated","time":"2021-04-15T10:36:42Z"}
pinot-server_1      | 2021/04/15 10:36:42.330 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:42.431 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pulsar_1            | 10:36:44.301 [pulsar-io-50-7] INFO  org.apache.pulsar.broker.service.ServerCnx - Closed connection from /192.168.32.1:64632
pinot-server_1      | 2021/04/15 10:36:45.533 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:45.634 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pinot-server_1      | 2021/04/15 10:36:48.737 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:48.837 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354572, startMessageId: 10:12:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354571, lastMessageId: 10:11:0
pulsar_1            | 10:36:49.238 [pulsar-io-50-8] INFO  org.apache.pulsar.broker.service.ServerCnx - New connection from /192.168.32.1:64638
pulsar_1            | 10:36:49.245 [pulsar-io-50-8] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64638][persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0] Creating producer. producerId=0
pulsar_1            | 10:36:49.247 [ForkJoinPool.commonPool-worker-0] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64638] persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0 configured with schema false
pulsar_1            | 10:36:49.248 [ForkJoinPool.commonPool-worker-0] INFO  org.apache.pulsar.broker.service.ServerCnx - [/192.168.32.1:64638] Created new producer: Producer{topic=PersistentTopic{topic=persistent://public/default/datasources-ingester-6078145ea64b0003002b3292-partition-0}, client=/192.168.32.1:64638, producerName=standalone-0-12, producerId=0}
pinot-server_1      | 2021/04/15 10:36:51.940 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 1 messages
pinot-server_1      | 2021/04/15 10:36:51.940 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Read message with id 10:12:0, offset 2684354572
pinot-server_1      | 2021/04/15 10:36:51.940 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Ack messages until 2684354572 offset, messageId 10:12:0
pinot-server_1      | 2021/04/15 10:36:51.944 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354573, startMessageId: 10:13:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354572, lastMessageId: 10:12:0
pulsar_1            | 10:36:53.198 [pulsar-web-68-6] INFO  org.eclipse.jetty.server.RequestLog - 127.0.0.1 - - [15/Apr/2021:10:36:53 +0000] "GET /admin/v2/persistent/public/functions/coordinate/stats?getPreciseBacklog=false&subscriptionBacklogSize=false HTTP/1.1" 200 1677 "-" "Pulsar-Java-v2.7.1" 4
pinot-server_1      | 2021/04/15 10:36:55.046 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] Found 0 messages
pinot-server_1      | 2021/04/15 10:36:55.147 INFO [PulsarPartitionLevelConsumer] [datasource_6078145ea64b0003002b3292__0__0__20210415T1024Z] fetchMessages() called, startOffset: 2684354573, startMessageId: 10:13:0, endOffset: 9223372036854775807, timeoutMillis: 5000, _lastOffsetReceived: 2684354572, lastMessageId: 10:12:0

       */
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }

    List<MessageAndOffset> messages = new ArrayList<>();
    try {
      // ack previous messages
      // if (_lastOffsetReceived != -1) {
      //   MessageId messageId = MessageIdUtils.getMessageId(_lastOffsetReceived, _partition);
      //   LOGGER.info("Ack messages until {} offset, messageId {}", _lastOffsetReceived, messageId); // TODO: move this to debug level
      //   _pulsarConsumer.acknowledgeCumulative(messageId);
      // }

      // read from pulsar
      Messages<byte[]> batchMessages = _pulsarConsumer.batchReceive();
      LOGGER.info("Found {} messages", batchMessages.size()); // TODO: move this to debug level
      if (batchMessages.size() == 0) { // stop here with no messages, and avoid any exception when trying to set lastOffsetReceived
        return new PulsarMessageBatch(Collections.emptyList());
      }

      // format for pinot
      for (Message<byte[]> record : batchMessages) {
        long offset = MessageIdUtils.getOffset(record.getMessageId());
        LOGGER.info("Read message with id {}, offset {}", record.getMessageId(), offset); // TODO: move this to debug level
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
      LOGGER.info("Ack messages until {} offset, messageId {}", _lastOffsetReceived, messageId); // TODO: move this to debug level
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
