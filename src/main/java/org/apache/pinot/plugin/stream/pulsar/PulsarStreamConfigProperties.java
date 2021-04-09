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

import com.google.common.base.Joiner;
import org.apache.pinot.spi.stream.StreamConfigProperties;


/**
 * Property key definitions for all Pulsar stream related properties
 */
public class PulsarStreamConfigProperties {
  public static final String DOT_SEPARATOR = ".";
  public static final String STREAM_TYPE = "pulsar";

  /**
   * Helper method to create a property string for Pulsar stream
   * @param property
   * @return
   */
  public static String constructStreamProperty(String property) {
    return Joiner.on(DOT_SEPARATOR).join(StreamConfigProperties.STREAM_PREFIX, property);
  }

  public static class LowLevelConsumer {
    public static final String PULSAR_BROKER_LIST = "pulsar.broker.list";
    public static final String PULSAR_CONSUMER_SIZE_BYTES = "pulsar.consumer.maxBytes";
    public static final int PULSAR_CONSUMER_SIZE_BYTES_DEFAULT = 1024 * 1024 * 10;
    public static final String PULSAR_CONSUMER_SIZE_MSGS = "pulsar.consumer.maxMsgs";
    public static final int PULSAR_CONSUMER_SIZE_MSGS_DEFAULT = -1;
    public static final String PULSAR_CONSUMER_TIMEOUT = "pulsar.consumer.timeout";
    public static final int PULSAR_CONSUMER_TIMEOUT_DEFAULT = 100;
  }

  public static final String PULSAR_CONSUMER_PROP_PREFIX = "pulsar.consumer.prop";
}

