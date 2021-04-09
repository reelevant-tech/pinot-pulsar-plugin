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
import java.util.Map;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamConfig;


/**
 * Wrapper around {@link StreamConfig} for use in {@link PulsarPartitionLevelConsumer}
 */
public class PulsarPartitionLevelStreamConfig {

  private final String _topicName;
  private final String _bootstrapHosts;
  private final Map<String, String> _streamConfigMap;
  private final int _batchMaxMsgs;
  private final int _batchMaxBytes;
  private final int _batchTimeout;

  /**
   * Builds a wrapper around {@link StreamConfig} to fetch PULSAR partition level consumer related configs
   * @param streamConfig
   */
  public PulsarPartitionLevelStreamConfig(StreamConfig streamConfig) {
    _streamConfigMap = streamConfig.getStreamConfigsMap();

    _topicName = streamConfig.getTopicName();

    String llcBrokerListKey = PulsarStreamConfigProperties
        .constructStreamProperty(PulsarStreamConfigProperties.LowLevelConsumer.PULSAR_BROKER_LIST);
    int llcMaxMsgs = getIntConfigWithDefault(_streamConfigMap, PulsarStreamConfigProperties.LowLevelConsumer.PULSAR_CONSUMER_SIZE_MSGS,
      PulsarStreamConfigProperties.LowLevelConsumer.PULSAR_CONSUMER_SIZE_MSGS_DEFAULT);
    int llcMaxSize = getIntConfigWithDefault(_streamConfigMap, PulsarStreamConfigProperties.LowLevelConsumer.PULSAR_CONSUMER_SIZE_BYTES,
      PulsarStreamConfigProperties.LowLevelConsumer.PULSAR_CONSUMER_SIZE_BYTES_DEFAULT);
    int llcTimeout = getIntConfigWithDefault(_streamConfigMap, PulsarStreamConfigProperties.LowLevelConsumer.PULSAR_CONSUMER_TIMEOUT,
        PulsarStreamConfigProperties.LowLevelConsumer.PULSAR_CONSUMER_TIMEOUT_DEFAULT);
    _bootstrapHosts = _streamConfigMap.get(llcBrokerListKey);
    _batchMaxBytes = llcMaxSize;
    _batchMaxMsgs = llcMaxMsgs;
    _batchTimeout = llcTimeout;

    Preconditions.checkNotNull(_bootstrapHosts,
        "Must specify PULSAR brokers list " + llcBrokerListKey + " in case of low level PULSAR consumer");
  }

  public String getTopicName() {
    return _topicName;
  }

  public String getBootstrapHosts() {
    return _bootstrapHosts;
  }

  public int getMaximumBatchSize() {
    return _batchMaxBytes;
  }

  public int getMaximumBatchMessagesCount() {
    return _batchMaxMsgs;
  }

  public int getBatchTimeout() {
    return _batchTimeout;
  }

  private int getIntConfigWithDefault(Map<String, String> configMap, String key, int defaultValue) {
    String stringValue = configMap.get(key);
    try {
      if (StringUtils.isNotEmpty(stringValue)) {
        return Integer.parseInt(stringValue);
      }
      return defaultValue;
    } catch (NumberFormatException ex) {
      return defaultValue;
    }
  }

  @Override
  public String toString() {
    return "PULSARLowLevelStreamConfig{" + "_pulsarTopicName='" + _topicName + '\'' + ", _bootstrapHosts='"
        + _bootstrapHosts + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    PulsarPartitionLevelStreamConfig that = (PulsarPartitionLevelStreamConfig) o;

    return EqualityUtils.isEqual(_topicName, that._topicName) && EqualityUtils
        .isEqual(_bootstrapHosts, that._bootstrapHosts);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_topicName);
    result = EqualityUtils.hashCodeOf(result, _bootstrapHosts);
    return result;
  }
}
