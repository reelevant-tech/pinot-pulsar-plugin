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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.MessageBatch;


/**
 * A {@link MessageBatch} for collecting messages from pulsar topic
 */
public class PulsarMessageBatch implements MessageBatch<byte[]> {

  private List<MessageAndOffset> _messageList = new ArrayList<>();

  public PulsarMessageBatch(List<MessageAndOffset> messages) {
    _messageList = messages;
  }

  @Override
  public int getMessageCount() {
    return _messageList.size();
  }

  @Override
  public byte[] getMessageAtIndex(int index) {
    return _messageList.get(index).getMessage().array();
  }

  @Override
  public int getMessageOffsetAtIndex(int index) {
    return _messageList.get(index).getMessage().arrayOffset();
  }

  @Override
  public int getMessageLengthAtIndex(int index) {
    return _messageList.get(index).payloadSize();
  }

  @Override
  public long getNextStreamMessageOffsetAtIndex(int index) {
    return _messageList.get(index).getNextOffset();
  }
}
