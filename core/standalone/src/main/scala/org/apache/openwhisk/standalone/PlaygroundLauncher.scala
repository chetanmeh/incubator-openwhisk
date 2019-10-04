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

package org.apache.openwhisk.standalone

import java.nio.charset.StandardCharsets.UTF_8

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentType, HttpCharsets, HttpEntity, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.FileAndResourceDirectives.ResourceFile
import akka.stream.ActorMaterializer
import org.apache.commons.io.IOUtils
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.http.BasicHttpService
import pureconfig.loadConfigOrThrow

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class PlaygroundLauncher(host: String, controllerPort: Int, pgPort: Int, authKey: String)(
  implicit logging: Logging,
  ec: ExecutionContext,
  actorSystem: ActorSystem,
  materializer: ActorMaterializer,
  tid: TransactionId) {
  private val interface = loadConfigOrThrow[String]("whisk.controller.interface")
  private val jsFileName = "playgroundFunctions.js"
  private val jsContentType = ContentType(MediaTypes.`application/javascript`, HttpCharsets.`UTF-8`)
  private val uiPath = {
    val res = getClass.getResource(s"/pg/ui/$jsFileName")
    Try(ResourceFile(res)) match {
      case Success(_) => "pg/ui"
      case Failure(_) => "BOOT-INF/classes/pg/ui"
    }
  }

  private val jsFileContent = {
    val js = resourceToString(jsFileName, "ui")
    val content = js.replace("window.APIHOST='http://localhost:3233'", s"window.APIHOST='http://$host:$controllerPort'")
    content.getBytes(UTF_8)
  }

  def run(): ServiceContainer = {
    BasicHttpService.startHttpService(PlaygroundService.route, pgPort, None, interface)(actorSystem, materializer)
    ServiceContainer(pgPort, s"http://${StandaloneDockerSupport.getLocalHostName()}:$pgPort", "Playground")
  }

  def install(): Unit = {}

  object PlaygroundService extends BasicHttpService {
    override def routes(implicit transid: TransactionId): Route =
      path("pg") { redirect(s"/pg/ui/playground.html", StatusCodes.Found) } ~
        pathPrefix("pg" / "ui" / Segment) { fileName =>
          get {
            if (fileName == jsFileName) {
              complete(HttpEntity(jsContentType, jsFileContent))
            } else {
              getFromResource(s"$uiPath/$fileName")
            }
          }
        }
  }

  private def resourceToString(name: String, resType: String) = IOUtils.resourceToString(s"/pg/$resType/$name", UTF_8)
}
