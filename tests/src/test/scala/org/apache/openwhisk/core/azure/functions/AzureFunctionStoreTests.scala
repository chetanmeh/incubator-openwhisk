/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.azure.functions

import common.WskActorSystem
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.azure.{AzureFunctionAction, AzureFunctionStoreProvider}
import org.apache.openwhisk.core.entity.DocRevision
import org.apache.openwhisk.core.entity.test.ExecHelpers
import org.junit.runner.RunWith
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{EitherValues, FlatSpec, Matchers}
import spray.json._

import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
class AzureFunctionStoreTests
    extends FlatSpec
    with Matchers
    with WskActorSystem
    with ScalaFutures
    with ExecHelpers
    with EitherValues {
  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = 60.seconds)
  implicit val tid = TransactionId.testing
  behavior of "invoke"

  val helloWorld = """function main(params) {
                     |    greeting = 'hello new2, ' + params.payload + '!'
                     |    console.log(greeting);
                     |    return {payload: greeting}
                     |}""".stripMargin

  val store = AzureFunctionStoreProvider.makeStore()

  ignore should "invoke function" in {
    val body = """{
                 |  "value": {
                 |    "payload" : "bar"
                 |  }
                 |}""".stripMargin.parseJson.asJsObject

    val func = AzureFunctionAction("foo", DocRevision.empty)
    val r = store.invokeFunction(func, body).futureValue
    println(r.response.right.get.entity)
  }

  ignore should "fetch bearer token" in {
    val token = store.fetchBearerToken().futureValue
    println(token)
    token.right.value should not be null
    println(token.right.get.access_token)
  }

  it should "invoke the actual function" in {
    val body = """{
                 |  "value": {
                 |    "payload" : "bar"
                 |  }
                 |}""".stripMargin.parseJson.asJsObject
    val result = store.invokeFunction(AzureFunctionAction("secondfunctionapp99", DocRevision.empty), body).futureValue
    println(result)
  }
}
