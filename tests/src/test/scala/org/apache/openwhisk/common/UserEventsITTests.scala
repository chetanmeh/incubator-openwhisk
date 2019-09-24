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

package org.apache.openwhisk.common

import akka.stream.ActorMaterializer
import common.{FreePortFinder, WskProps}
import org.apache.openwhisk.standalone.StandaloneServerFixture

class UserEventsITTests extends UserEventTests with StandaloneServerFixture {
  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private val kafkaPort = sys.props.get("whisk.kafka.port").map(_.toInt).getOrElse(FreePortFinder.freePort())

  protected override val customConfig = Some("""
      |include classpath("standalone.conf")
      |whisk {
      |  user-events {
      |    enabled = true
      |  }
      |}
      """.stripMargin)

  override protected def extraArgs: Seq[String] =
    Seq("--kafka", "--dev-mode", "--kafka-port", kafkaPort.toString)

  override implicit val wskprops = WskProps().copy(apihost = serverUrl)

  override def userEventsEnabled = true

  override def kafkaHosts = s"localhost:$kafkaPort"
}
