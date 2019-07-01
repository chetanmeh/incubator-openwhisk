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

import java.net.InetSocketAddress
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{ClientTransport, Http}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.containerpool.{Interval, RunResult}
import org.apache.openwhisk.core.entity.ActivationResponse.{ConnectionError, ContainerResponse}
import org.apache.openwhisk.core.entity.{DocRevision, FullyQualifiedEntityName}
import pureconfig.loadConfigOrThrow
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object AzureFunctionStoreProvider {
  def makeStore(config: Config = ConfigFactory.defaultApplication())(implicit system: ActorSystem,
                                                                     ec: ExecutionContext,
                                                                     logging: Logging): AzureFunctionStore = {
    val funcConfig = loadConfigOrThrow[AzureFunctionConfig](config, ConfigKeys.azureFunctions)
    val azureConfig = loadConfigOrThrow[AzureConfig](config, ConfigKeys.azure)
    new AzureFunctionStore(funcConfig, azureConfig)
  }
}

case class AzureConfig(subscriptionId: String, resourceGroup: String, tenantId: String, credentials: AzureCredentials)
case class AzureCredentials(clientId: String, clientSecret: String)
case class AzureFunctionConfig(httpTriggerName: String, authKey: String)

case class AzureFunctionAction(name: String, whiskRevision: DocRevision)

class AzureFunctionStore(funcConfig: AzureFunctionConfig, azureConfig: AzureConfig)(implicit system: ActorSystem,
                                                                                    ec: ExecutionContext,
                                                                                    logging: Logging) {
  import AzureClient._
  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private val httpSettings = clientSettings(system)

  def getFunction(kind: String, fqn: FullyQualifiedEntityName)(
    implicit transid: TransactionId): Future[Option[AzureFunctionAction]] = {
    //TODO Eventually we need to list for deployed function and check if the function exist
    //For now using a basic heuristic
    if (kind == "nodejs:8") Future.successful(Some(AzureFunctionAction(fqn.name.name, DocRevision.empty)))
    else Future.successful(None)
  }

  def invokeFunction(action: AzureFunctionAction, body: JsObject)(
    implicit transid: TransactionId): Future[RunResult] = {
    val started = Instant.now()
    //TODO Switch to logic as per in AkkaContainerClient and avoid cost in unmarshalling to json
    runFunction(action.name, body)
      .map {
        case Right(js) =>
          Right(ContainerResponse(OK.intValue, js.compactPrint, None))
        case Left(status) =>
          Right(ContainerResponse(status.intValue, status.reason(), None))
      }
      .recover {
        case NonFatal(t) => Left(ConnectionError(t))
      }
      .map { response =>
        val finished = Instant.now()
        RunResult(Interval(started, finished), response)
      }
  }

  def fetchBearerToken(): Future[Either[StatusCode, BearerToken]] = {
    requestJson[BearerToken](
      mkFormRequest(
        HttpMethods.POST,
        Uri(s"https://login.microsoftonline.com/${azureConfig.tenantId}/oauth2/token"),
        Map(
          "grant_type" -> "client_credentials",
          "client_id" -> azureConfig.credentials.clientId,
          "resource" -> "https://management.core.windows.net/",
          "client_secret" -> azureConfig.credentials.clientSecret)))
  }

  def runFunction(funcName: String, body: JsObject): Future[Either[StatusCode, JsObject]] = {
    requestJson[JsObject](
      mkJsonRequest(
        HttpMethods.POST,
        Uri(s"https://$funcName.azurewebsites.net/api/${funcConfig.httpTriggerName}"),
        body,
        List(RawHeader("x-functions-key", funcConfig.authKey))))
  }

  /**
   * Execute an HttpRequest on the underlying connection pool and return an unmarshalled result.
   *
   * @return either the unmarshalled result or a status code, if the status code is not a success (2xx class)
   */
  def requestJson[T: RootJsonReader](futureRequest: Future[HttpRequest]): Future[Either[StatusCode, T]] =
    futureRequest.flatMap { request =>
      Http().singleRequest(request, settings = httpSettings).flatMap { response =>
        if (response.status.isSuccess) {
          Unmarshal(response.entity.withoutSizeLimit).to[T].map(Right.apply)
        } else {
          Unmarshal(response.entity).to[String].flatMap { body =>
            val statusCode = response.status
            val reason =
              if (body.nonEmpty) s"${statusCode.reason} (details: $body)" else statusCode.reason
            val customStatusCode = StatusCodes
              .custom(intValue = statusCode.intValue, reason = reason, defaultMessage = statusCode.defaultMessage)
            // This is important, as it drains the entity stream.
            // Otherwise the connection stays open and the pool dries up.
            response.discardEntityBytes().future.map(_ => Left(customStatusCode))
          }
        }
      }
    }

}

object AzureClient {

  def mkRequest(method: HttpMethod,
                uri: Uri,
                body: Future[MessageEntity] = Future.successful(HttpEntity.Empty),
                headers: List[HttpHeader] = List.empty)(implicit ec: ExecutionContext): Future[HttpRequest] = {
    body.map { b =>
      HttpRequest(method, uri, headers, b)
    }
  }

  def mkJsonRequest(method: HttpMethod, uri: Uri, body: JsValue, headers: List[HttpHeader] = List.empty)(
    implicit ec: ExecutionContext): Future[HttpRequest] = {
    val b = Marshal(body).to[MessageEntity]
    mkRequest(method, uri, b, headers)
  }

  def mkFormRequest(method: HttpMethod, uri: Uri, body: Map[String, String], headers: List[HttpHeader] = List.empty)(
    implicit ec: ExecutionContext): Future[HttpRequest] = {
    val b = Future.successful(FormData(body).toEntity)
    mkRequest(method, uri, b, headers)
  }

  def clientSettings(system: ActorSystem): ConnectionPoolSettings = {
    sys.env.get("HTTPS_PROXY") match {
      case Some(p) =>
        val u = Uri(p)
        val proxyTransport =
          ClientTransport.httpsProxy(InetSocketAddress.createUnresolved(u.authority.host.address(), u.authority.port))
        ConnectionPoolSettings(system).withConnectionSettings(
          ClientConnectionSettings(system).withTransport(proxyTransport))
      case None => ConnectionPoolSettings(system)
    }
  }
}
