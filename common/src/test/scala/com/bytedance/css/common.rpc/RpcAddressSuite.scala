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

package com.bytedance.css.common.rpc

import com.bytedance.css.common.exception.CssException
import org.scalatest.FunSuite

class RpcAddressSuite extends FunSuite {

  test("hostPort") {
    val address = RpcAddress("1.2.3.4", 1234)
    assert(address.host == "1.2.3.4")
    assert(address.port == 1234)
    assert(address.hostPort == "1.2.3.4:1234")
  }

  test("fromCssURL") {
    val address = RpcAddress.fromCssURL("css://1.2.3.4:1234")
    assert(address.host == "1.2.3.4")
    assert(address.port == 1234)
  }

  test("fromCssURL: a typo url") {
    val e = intercept[CssException] {
      RpcAddress.fromCssURL("css://1.2. 3.4:1234")
    }
    assert("Invalid master URL: css://1.2. 3.4:1234" === e.getMessage)
  }

  test("fromCssURL: invalid scheme") {
    val e = intercept[CssException] {
      RpcAddress.fromCssURL("invalid://1.2.3.4:1234")
    }
    assert("Invalid master URL: invalid://1.2.3.4:1234" === e.getMessage)
  }

  test("toCssURL") {
    val address = RpcAddress("1.2.3.4", 1234)
    assert(address.toCssURL == "css://1.2.3.4:1234")
  }
}
