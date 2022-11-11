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

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.EqualityUtils;


/**
 * Wrapper around {@link StreamConfig} for use in {@link PulsarPartitionLevelConsumer}
 */
public class PulsarPartitionLevelStreamConfig {

  public static final String PULSAR_BROKER_LIST = "pulsar.broker.list";
  public static final String PULSAR_CONSUMER_SIZE_BYTES = "pulsar.consumer.maxBytes";
  public static final int PULSAR_CONSUMER_SIZE_BYTES_DEFAULT = 1024 * 1024 * 10;
  public static final String PULSAR_CONSUMER_SIZE_MSGS = "pulsar.consumer.maxMsgs";
  public static final int PULSAR_CONSUMER_SIZE_MSGS_DEFAULT = 500;
  public static final String PULSAR_CONSUMER_TIMEOUT = "pulsar.consumer.timeout";
  public static final int PULSAR_CONSUMER_TIMEOUT_DEFAULT = 100;
  public static final String STREAM_TYPE = "pulsar";
  

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
    String llcMaxMsgsKey = StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PULSAR_CONSUMER_SIZE_MSGS);
    int llcMaxMsgs = getIntConfigWithDefault(_streamConfigMap, llcMaxMsgsKey, PULSAR_CONSUMER_SIZE_MSGS_DEFAULT);
    String llcMaxSizeKey = StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PULSAR_CONSUMER_SIZE_BYTES);
    int llcMaxSize = getIntConfigWithDefault(_streamConfigMap, llcMaxSizeKey, PULSAR_CONSUMER_SIZE_BYTES_DEFAULT);
    String llcTimeoutKey = StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PULSAR_CONSUMER_TIMEOUT);
    int llcTimeout = getIntConfigWithDefault(_streamConfigMap, llcTimeoutKey, PULSAR_CONSUMER_TIMEOUT_DEFAULT);

    String llcBrokerListKey = StreamConfigProperties
        .constructStreamProperty(STREAM_TYPE, PULSAR_BROKER_LIST);
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
