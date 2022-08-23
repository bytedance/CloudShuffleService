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

package com.bytedance.css.common.rpc.netty

import java.nio.ByteBuffer

import scala.concurrent.Promise

import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.rpc.{RpcAddress, RpcCallContext}
import com.bytedance.css.network.client.RpcResponseCallback

abstract class NettyRpcCallContext(override val senderAddress: RpcAddress)
  extends RpcCallContext with Logging {

  protected def send(message: Any): Unit

  override def reply(response: Any): Unit = {
    send(response)
  }

  override def sendFailure(e: Throwable): Unit = {
    send(RpcFailure(e))
  }

}

/**
 * If the sender and the receiver are in the same process, the reply can be sent back via `Promise`.
 */
class LocalNettyRpcCallContext(
    senderAddress: RpcAddress,
    p: Promise[Any])
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    p.success(message)
  }
}

/**
 * A [[RpcCallContext]] that will call [[RpcResponseCallback]] to send the reply back.
 */
class RemoteNettyRpcCallContext(
    val nettyEnv: NettyRpcEnv,
    callback: RpcResponseCallback,
    senderAddress: RpcAddress)
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    val reply: ByteBuffer = if (message.isInstanceOf[ByteBuffer]) {
      message.asInstanceOf[ByteBuffer]
    } else {
      nettyEnv.serialize(message)
    }
    callback.onSuccess(reply)
  }
}
