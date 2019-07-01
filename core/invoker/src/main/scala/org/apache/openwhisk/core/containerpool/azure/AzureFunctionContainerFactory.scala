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

package org.apache.openwhisk.core.containerpool.azure

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.core.azure.{AzureFunctionAction, AzureFunctionStoreProvider}
import org.apache.openwhisk.core.containerpool.{Container, ContainerFactory, ContainerFactoryProvider}
import org.apache.openwhisk.core.entity.{ByteSize, ExecManifest, ExecutableWhiskAction, InvokerInstanceId}
import org.apache.openwhisk.spi.{SpiClassResolver, SpiLoader}

import scala.concurrent.{ExecutionContext, Future}

class AzureFunctionContainerFactory(
  instance: InvokerInstanceId,
  parameters: Map[String, Set[String]],
  whiskConfig: WhiskConfig)(implicit actorSystem: ActorSystem, ec: ExecutionContext, logging: Logging)
    extends ContainerFactory {
  private val secondaryFactory = loadSecondaryFactory()
  private val azureFuncStore = AzureFunctionStoreProvider.makeStore()

  logging.info(this, "Initializing AzureContainerFactory")

  override def createContainer(
    tid: TransactionId,
    name: String,
    actionImage: ExecManifest.ImageName,
    userProvidedImage: Boolean,
    memory: ByteSize,
    cpuShares: Int,
    actionOpt: Option[ExecutableWhiskAction])(implicit config: WhiskConfig, logging: Logging): Future[Container] = {
    implicit val tidi = tid
    def createSecondary(): Future[Container] =
      secondaryFactory.createContainer(tid, name, actionImage, userProvidedImage, memory, cpuShares, actionOpt)
    actionOpt
      .map { action =>
        val actionRev = action.rev
        //TODO Cache the mapping with key as FQN + docRev
        val f = azureFuncStore.getFunction(action.fullyQualifiedName(false)).flatMap {
          case Some(l @ AzureFunctionAction(_, `actionRev`)) =>
            Future.successful(AzureFunctionContainer(l, azureFuncStore))
          case Some(l) =>
            logging.info(
              this,
              s"Azure function whisk revision $l did not matched expected revision ${actionInfo(action)}. Delegating to secondary factory")
            createSecondary()
          case _ =>
            logging
              .info(this, s"No matching azure function found for ${actionInfo(action)}")
            createSecondary()
        }
        f.failed.foreach(t => logging.warn(this, s"Error occurred while invoking Azure Function API $t"))
        f
      }
      .getOrElse(createSecondary())
  }
  override def init(): Unit = secondaryFactory.init()
  override def cleanup(): Unit = secondaryFactory.cleanup()

  private def loadSecondaryFactory(config: Config = ConfigFactory.load()): ContainerFactory = {
    val secondaryFactory = config.getString(s"${ConfigKeys.azureFunctions}.secondary-factory-provider")
    implicit val spiResolver = SimpleResolver(secondaryFactory)
    SpiLoader
      .get[ContainerFactoryProvider]
      .instance(actorSystem, logging, whiskConfig, instance, parameters)
  }

  private case class SimpleResolver(value: String) extends SpiClassResolver {
    override def getClassNameForType[T: Manifest]: String = value
  }

  private def actionInfo(action: ExecutableWhiskAction) = s"${action.fullyQualifiedName(false)}/${action.rev}"
}

object AzureContainerFactoryProvider extends ContainerFactoryProvider {
  override def instance(actorSystem: ActorSystem,
                        logging: Logging,
                        config: WhiskConfig,
                        instanceId: InvokerInstanceId,
                        parameters: Map[String, Set[String]]): ContainerFactory = {

    new AzureFunctionContainerFactory(instanceId, parameters, config)(actorSystem, actorSystem.dispatcher, logging)
  }
}
