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

package org.apache.openwhisk.core.azure

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.containerpool.RunResult
import org.apache.openwhisk.core.entity.{DocRevision, FullyQualifiedEntityName}
import pureconfig.loadConfigOrThrow
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

object AzureFunctionStoreProvider {
  def makeStore(config: Config = ConfigFactory.defaultApplication())(implicit ec: ExecutionContext,
                                                                     logging: Logging): AzureFunctionStore = {
    val lambdaConfig = loadConfigOrThrow[AzureFunctionConfig](config, ConfigKeys.azureFunctions)
    new AzureFunctionStore(lambdaConfig)
  }
}

case class AzureFunctionConfig()

case class AzureFunctionAction(name: String, whiskRevision: DocRevision)

class AzureFunctionStore(config: AzureFunctionConfig)(implicit ec: ExecutionContext, logging: Logging) {

  def getFunction(fqn: FullyQualifiedEntityName)(
    implicit transid: TransactionId): Future[Option[AzureFunctionAction]] = {
    ???
  }

  def invokeFunction(action: AzureFunctionAction, body: JsObject)(implicit transid: TransactionId): Future[RunResult] =
    ???
}
