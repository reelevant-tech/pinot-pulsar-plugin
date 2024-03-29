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
import java.util.Set;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarConsumerFactory extends StreamConsumerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConsumerFactory.class);

  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    try {
      return new PulsarPartitionLevelConsumer(clientId, _streamConfig, partition);
    } catch (IOException e) {
      LOGGER.error("Could not connect to pulsar", e);
      return null;
    }
  }

  @Override
  public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Set<String> fieldsToRead,
      String groupId) {
    return null;
  }

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    try {
      return new PulsarStreamMetadataProvider(clientId, _streamConfig, partition);
    } catch (IOException e) {
      LOGGER.error("Could not connect to pulsar", e);
      return null;
    }
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    try {
      return new PulsarStreamMetadataProvider(clientId, _streamConfig);
    } catch (IOException e) {
      LOGGER.error("Could not connect to pulsar", e);
      return null;
    }
  }
}
