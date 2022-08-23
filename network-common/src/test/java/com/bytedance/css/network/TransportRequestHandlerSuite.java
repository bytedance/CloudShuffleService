/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file may have been modified by Bytedance Inc.
 */

package com.bytedance.css.network;

import com.bytedance.css.network.protocol.RequestMessage;
import com.bytedance.css.network.protocol.StreamFailure;
import com.bytedance.css.network.protocol.StreamRequest;
import com.bytedance.css.network.protocol.StreamResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import com.bytedance.css.network.buffer.ManagedBuffer;
import com.bytedance.css.network.client.TransportClient;
import com.bytedance.css.network.server.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class TransportRequestHandlerSuite {

  @Test
  public void handleStreamRequest() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs =
      new ArrayList<>();
    when(channel.writeAndFlush(any()))
      .thenAnswer(invocationOnMock0 -> {
        Object response = invocationOnMock0.getArguments()[0];
        ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
        responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
        return channelFuture;
      });

    // Prepare the stream.
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    managedBuffers.add(new TestManagedBuffer(20));
    managedBuffers.add(null);
    managedBuffers.add(new TestManagedBuffer(30));
    managedBuffers.add(new TestManagedBuffer(40));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    Assert.assertEquals(1, streamManager.numStreamStates());

    TransportClient reverseClient = mock(TransportClient.class);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, reverseClient,
      rpcHandler, 2L, null);

    RequestMessage request0 = new StreamRequest(String.format("%d_%d", streamId, 0));
    requestHandler.handle(request0);
    Assert.assertEquals(1, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof StreamResponse);
    Assert.assertEquals(managedBuffers.get(0),
      ((StreamResponse) (responseAndPromisePairs.get(0).getLeft())).body());

    RequestMessage request1 = new StreamRequest(String.format("%d_%d", streamId, 1));
    requestHandler.handle(request1);
    Assert.assertEquals(2, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(1).getLeft() instanceof StreamResponse);
    Assert.assertEquals(managedBuffers.get(1),
      ((StreamResponse) (responseAndPromisePairs.get(1).getLeft())).body());

    // Finish flushing the response for request0.
    responseAndPromisePairs.get(0).getRight().finish(true);

    StreamRequest request2 = new StreamRequest(String.format("%d_%d", streamId, 2));
    requestHandler.handle(request2);
    Assert.assertEquals(3, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(2).getLeft() instanceof StreamFailure);
    Assert.assertEquals(String.format("Stream '%s' was not found.", request2.streamId),
        ((StreamFailure) (responseAndPromisePairs.get(2).getLeft())).error);

    RequestMessage request3 = new StreamRequest(String.format("%d_%d", streamId, 3));
    requestHandler.handle(request3);
    Assert.assertEquals(4, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(3).getLeft() instanceof StreamResponse);
    Assert.assertEquals(managedBuffers.get(3),
      ((StreamResponse) (responseAndPromisePairs.get(3).getLeft())).body());

    // Request4 will trigger the close of channel, because the number of max chunks being
    // transferred is 2;
    RequestMessage request4 = new StreamRequest(String.format("%d_%d", streamId, 4));
    requestHandler.handle(request4);
    verify(channel, times(1)).close();
    Assert.assertEquals(4, responseAndPromisePairs.size());

    streamManager.connectionTerminated(channel);
    Assert.assertEquals(0, streamManager.numStreamStates());
  }

  private class ExtendedChannelPromise extends DefaultChannelPromise {

    private List<GenericFutureListener<Future<Void>>> listeners = new ArrayList<>();
    private boolean success;

    ExtendedChannelPromise(Channel channel) {
      super(channel);
      success = false;
    }

    @Override
    public ChannelPromise addListener(
      GenericFutureListener<? extends Future<? super Void>> listener) {
      @SuppressWarnings("unchecked")
      GenericFutureListener<Future<Void>> gfListener =
        (GenericFutureListener<Future<Void>>) listener;
      listeners.add(gfListener);
      return super.addListener(listener);
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    public void finish(boolean success) {
      this.success = success;
      listeners.forEach(listener -> {
        try {
          listener.operationComplete(this);
        } catch (Exception e) {
          // do nothing
        }
      });
    }
  }
}
