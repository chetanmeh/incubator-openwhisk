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

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.containerpool.RunResult
import org.apache.openwhisk.core.entity.{DocRevision, FullyQualifiedEntityName}
import pureconfig.loadConfigOrThrow
import spray.json.{JsObject, JsValue, RootJsonReader}

import scala.concurrent.{ExecutionContext, Future}

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
case class AzureFunctionConfig()

case class AzureFunctionAction(name: String, whiskRevision: DocRevision)

class AzureFunctionStore(funcConfig: AzureFunctionConfig, azureConfig: AzureConfig)(implicit system: ActorSystem,
                                                                                    ec: ExecutionContext,
                                                                                    logging: Logging) {
  import AzureClient._
  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  def getFunction(fqn: FullyQualifiedEntityName)(
    implicit transid: TransactionId): Future[Option[AzureFunctionAction]] = {
    ???
  }

  def invokeFunction(action: AzureFunctionAction, body: JsObject)(implicit transid: TransactionId): Future[RunResult] =
    ???

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

  /**
   * Execute an HttpRequest on the underlying connection pool and return an unmarshalled result.
   *
   * @return either the unmarshalled result or a status code, if the status code is not a success (2xx class)
   */
  def requestJson[T: RootJsonReader](futureRequest: Future[HttpRequest]): Future[Either[StatusCode, T]] =
    futureRequest.flatMap { request =>
      Http().singleRequest(request).flatMap { response =>
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
}
