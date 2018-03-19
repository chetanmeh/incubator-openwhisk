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

package whisk.core.database.couchdb

import akka.actor.ActorSystem
import akka.event.Logging.ErrorLevel
import akka.http.scaladsl.model.{ContentType, StatusCodes}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import pureconfig.loadConfigOrThrow
import spray.json.DefaultJsonProtocol
import whisk.common.{Logging, LoggingMarkers, TransactionId}
import whisk.core.ConfigKeys
import whisk.core.database._
import whisk.core.entity.DocInfo

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

object CouchDbAttachmentStoreProvider extends AttachmentStoreProvider {
  override def makeStore[D <: DocumentSerializer: ClassTag]()(implicit actorSystem: ActorSystem,
                                                              logging: Logging,
                                                              materializer: ActorMaterializer): AttachmentStore = {
    val config = loadConfigOrThrow[CouchDbConfig](ConfigKeys.couchdb)
    new CouchDbAttachmentStore(
      config.protocol,
      config.host,
      config.port,
      config.username,
      config.password,
      config.databaseFor[D])
  }
}

class CouchDbAttachmentStore(
  dbProtocol: String,
  dbHost: String,
  dbPort: Int,
  dbUsername: String,
  dbPassword: String,
  dbName: String)(implicit system: ActorSystem, logging: Logging, materializer: ActorMaterializer)
    extends AttachmentStore
    with DefaultJsonProtocol {

  /** Execution context for futures */
  override protected[core] implicit val executionContext: ExecutionContext = system.dispatcher

  private val client: CouchDbRestClient =
    new CouchDbRestClient(dbProtocol, dbHost, dbPort, dbUsername, dbPassword, dbName)

  override protected[core] def attach(
    doc: DocInfo,
    name: String,
    contentType: ContentType,
    docStream: Source[ByteString, _])(implicit transid: TransactionId): Future[DocInfo] = {

    val start = transid.started(
      this,
      LoggingMarkers.DATABASE_ATT_SAVE,
      s"[ATT_PUT] '$dbName' uploading attachment '$name' of document '$doc'")

    require(doc != null, "doc undefined")
    require(doc.rev.rev != null, "doc revision must be specified")

    val f = client.putAttachment(doc.id.id, doc.rev.rev, name, contentType, docStream).map {
      case Right(response) =>
        transid
          .finished(this, start, s"[ATT_PUT] '$dbName' completed uploading attachment '$name' of document '$doc'")
        val id = response.fields("id").convertTo[String]
        val rev = response.fields("rev").convertTo[String]
        DocInfo ! (id, rev)

      case Left(StatusCodes.NotFound) =>
        transid
          .finished(this, start, s"[ATT_PUT] '$dbName' uploading attachment '$name' of document '$doc'; not found")
        throw NoDocumentException("Not found on 'readAttachment'.")

      case Left(code) =>
        transid.failed(
          this,
          start,
          s"[ATT_PUT] '$dbName' failed to upload attachment '$name' of document '$doc'; http status '$code'")
        throw new Exception("Unexpected http response code: " + code)
    }

    reportFailure(
      f,
      failure =>
        transid.failed(
          this,
          start,
          s"[ATT_PUT] '$dbName' internal error, name: '$name', doc: '$doc', failure: '${failure.getMessage}'",
          ErrorLevel))
  }

  override protected[core] def readAttachment[T](doc: DocInfo, name: String, sink: Sink[ByteString, Future[T]])(
    implicit transid: TransactionId): Future[(ContentType, T)] = {

    val start = transid.started(
      this,
      LoggingMarkers.DATABASE_ATT_GET,
      s"[ATT_GET] '$dbName' finding attachment '$name' of document '$doc'")

    require(doc != null, "doc undefined")
    require(doc.rev.rev != null, "doc revision must be specified")

    val f = client.getAttachment[T](doc.id.id, doc.rev.rev, name, sink)
    val g = f.map {
      case Right((contentType, result)) =>
        transid.finished(this, start, s"[ATT_GET] '$dbName' completed: found attachment '$name' of document '$doc'")
        (contentType, result)

      case Left(StatusCodes.NotFound) =>
        transid.finished(
          this,
          start,
          s"[ATT_GET] '$dbName', retrieving attachment '$name' of document '$doc'; not found.")
        throw NoDocumentException("Not found on 'readAttachment'.")

      case Left(code) =>
        transid.failed(
          this,
          start,
          s"[ATT_GET] '$dbName' failed to get attachment '$name' of document '$doc'; http status: '$code'")
        throw new Exception("Unexpected http response code: " + code)
    }

    reportFailure(
      g,
      failure =>
        transid.failed(
          this,
          start,
          s"[ATT_GET] '$dbName' internal error, name: '$name', doc: '$doc', failure: '${failure.getMessage}'",
          ErrorLevel))
  }

  override protected[core] def deleteAttachments(doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] =
    // NOTE: this method is not intended for standalone use for CouchDB.
    // To delete attachments, it is expected that the entire document is deleted.
    Future.successful(true)

  private def reportFailure[T, U](f: Future[T], onFailure: Throwable => U): Future[T] = {
    f.onFailure({
      case _: ArtifactStoreException => // These failures are intentional and shouldn't trigger the catcher.
      case x                         => onFailure(x)
    })
    f
  }
}
