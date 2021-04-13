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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * KafkaPartitionLevelConnectionHandler provides low level APIs to access Kafka partition level information.
 * E.g. partition counts, offsets per partition.
 *
 */
public abstract class PulsarPartitionLevelConnectionHandler {

  protected final PulsarPartitionLevelStreamConfig _config;
  protected final String _clientId;
  protected final int _partition;
  protected final String _topic;
  protected final PulsarClient _pulsarClient;
  protected Consumer<byte[]> _pulsarConsumer = null;
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);

  public PulsarPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig, int partition) throws IOException {
    LOGGER.info("Construct new PulsarPartitionLevelConnectionHandler, clientId: {}, streamConfig: {}, partition: {}", clientId, streamConfig, partition);
    _config = new PulsarPartitionLevelStreamConfig(streamConfig);
    _clientId = clientId;
    _partition = partition;
    _topic = _config.getTopicName();
    _pulsarClient = PulsarClient
      .builder()
      .serviceUrl(_config.getBootstrapHosts())
      .build();
    // 
    if (partition == Integer.MIN_VALUE) return;
    String topic = _topic + "-partition-" + partition;
    _pulsarConsumer = _pulsarClient
      .newConsumer()
      .topic(topic)
      .subscriptionName(topic)
      .consumerName(topic)
      .subscriptionType(SubscriptionType.Exclusive)
      .enableBatchIndexAcknowledgment(false)
      .batchReceivePolicy(BatchReceivePolicy.builder()
        .maxNumMessages(_config.getMaximumBatchMessagesCount())
        .maxNumBytes(_config.getMaximumBatchSize())
        .timeout(_config.getBatchTimeout(), TimeUnit.MILLISECONDS)
        .build())
      .subscribe();
  }

  // note: this method can be called by Pinot if we don't receive any messages in a long time
  // see https://github.com/apache/incubator-pinot/blob/89a22f097c5ff26396e58950c90d764066a56121/pinot-core/src/main/java/org/apache/pinot/core/data/manager/realtime/LLRealtimeSegmentDataManager.java#L413-L414
  public void close()
      throws IOException {
    LOGGER.info("Close PulsarPartitionLevelConnectionHandler, clientId: {}, topic: {}, partition: {}", _clientId, _topic, _partition);
    if (_pulsarConsumer != null) {
      _pulsarConsumer.close();
    } 
    _pulsarClient.close();
  }

  @VisibleForTesting
  protected PulsarPartitionLevelStreamConfig getPulsarPartitionLevelStreamConfig() {
    return _config;
  }
}
