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

package whisk.core.database.cosmosdb

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentType, StatusCodes}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.microsoft.azure.cosmosdb.internal.Constants.Properties.{E_TAG, ID, SELF_LINK}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.microsoft.azure.cosmosdb.{AccessCondition, Document, DocumentClientException, RequestOptions}
import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, RootJsonFormat, _}
import whisk.common.{Logging, LoggingMarkers, TransactionId}
import whisk.core.database.StoreUtils.{checkDocHasRevision, deserialize, reportFailure}
import whisk.core.database._
import whisk.core.database.cosmosdb.CosmosDBArtifactStore._computed
import whisk.core.database.cosmosdb.CosmosDBArtifactStoreProvider.DocumentClientRef
import whisk.core.entity._
import whisk.http.Messages

import scala.concurrent.{ExecutionContext, Future}

object CosmosDBArtifactStore {
  val _computed = "_c"
}

class CosmosDBArtifactStore[DocumentAbstraction <: DocumentSerializer](protected val collName: String,
                                                                       protected val config: CosmosDBConfig,
                                                                       clientRef: DocumentClientRef,
                                                                       documentHandler: DocumentHandler,
                                                                       viewMapper: CosmosDBViewMapper)(
  implicit system: ActorSystem,
  val logging: Logging,
  jsonFormat: RootJsonFormat[DocumentAbstraction],
  materializer: ActorMaterializer,
  docReader: DocumentReader)
    extends ArtifactStore[DocumentAbstraction]
    with DefaultJsonProtocol
    with DocumentProvider
    with CosmosDBSupport
    with RxObservableImplicits {

  protected val client: AsyncDocumentClient = clientRef.get.client
  private val (database, collection) = initialize()

  private val _id = "_id"
  private val _rev = "_rev"

  override protected[core] implicit val executionContext: ExecutionContext = system.dispatcher

  override protected[database] def put(d: DocumentAbstraction)(implicit transid: TransactionId): Future[DocInfo] = {
    val asJson = d.toDocumentRecord

    //TODO Batching support
    val doc = toCosmosDoc(asJson)
    val docinfoStr = s"id: ${doc.getId}, rev: ${doc.getETag}"
    val start = transid.started(this, LoggingMarkers.DATABASE_SAVE, s"[PUT] '$collName' saving document: '$docinfoStr'")

    val o = if (doc.getETag == null) {
      client.createDocument(collection.getSelfLink, doc, null, true)
    } else {
      client.replaceDocument(doc, matchRevOption(doc.getETag))
    }

    val f = o
      .head()
      .transform(
        r => toDocInfo(r.getResource), {
          case e: DocumentClientException
              if e.getStatusCode == StatusCodes.Conflict.intValue || e.getStatusCode == StatusCodes.PreconditionFailed.intValue =>
            DocumentConflictException("conflict on 'put'")
          case e => e
        })

    f.onFailure({
      case _: DocumentConflictException =>
        transid.finished(this, start, s"[PUT] '$collName', document: '$docinfoStr'; conflict.")
    })

    f.onSuccess({
      case _ => transid.finished(this, start, s"[PUT] '$collName' completed document: '$docinfoStr'")
    })

    reportFailure(f, start, failure => s"[PUT] '$collName' internal error, failure: '${failure.getMessage}'")
  }

  override protected[database] def del(doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] = {
    checkDocHasRevision(doc)
    val start = transid.started(this, LoggingMarkers.DATABASE_DELETE, s"[DEL] '$collName' deleting document: '$doc'")
    val f = client
      .deleteDocument(createSelfLink(doc.id.id), matchRevOption(doc.rev.rev))
      .head()
      .transform(
        _ => true, {
          case e: DocumentClientException if e.getStatusCode == StatusCodes.NotFound.intValue =>
            NoDocumentException("not found on 'delete'")
          case e: DocumentClientException if e.getStatusCode == StatusCodes.PreconditionFailed.intValue =>
            DocumentConflictException("conflict on 'delete'")
          case e => e
        })

    reportFailure(
      f,
      start,
      failure => s"[DEL] '$collName' internal error, doc: '$doc', failure: '${failure.getMessage}'")
  }

  override protected[database] def get[A <: DocumentAbstraction](doc: DocInfo)(implicit transid: TransactionId,
                                                                               ma: Manifest[A]): Future[A] = {
    val start = transid.started(this, LoggingMarkers.DATABASE_GET, s"[GET] '$collName' finding document: '$doc'")

    require(doc != null, "doc undefined")
    val f = client
      .readDocument(createSelfLink(doc.id.id), null)
      .head()
      .transform(
        { rr =>
          val js = toWhiskJsonDoc(rr.getResource)
          transid.finished(this, start, s"[GET] '$collName' completed: found document '$doc'")
          deserialize[A, DocumentAbstraction](doc, js)
        }, {
          case e: DocumentClientException if e.getStatusCode == StatusCodes.NotFound.intValue =>
            transid.finished(this, start, s"[GET] '$collName', document: '${doc}'; not found.")
            // for compatibility
            throw NoDocumentException("not found on 'get'")
          case e => e
        })
      .recoverWith {
        case _: DeserializationException => throw DocumentUnreadable(Messages.corruptedEntity)
      }

    reportFailure(
      f,
      start,
      failure => s"[DEL] '$collName' internal error, doc: '$doc', failure: '${failure.getMessage}'")

  }

  override protected[core] def query(table: String,
                                     startKey: List[Any],
                                     endKey: List[Any],
                                     skip: Int,
                                     limit: Int,
                                     includeDocs: Boolean,
                                     descending: Boolean,
                                     reduce: Boolean,
                                     stale: StaleParameter)(implicit transid: TransactionId): Future[List[JsObject]] =
    ???

  override protected[core] def count(table: String,
                                     startKey: List[Any],
                                     endKey: List[Any],
                                     skip: Int,
                                     stale: StaleParameter)(implicit transid: TransactionId): Future[Long] = ???

  override protected[core] def attach(
    doc: DocInfo,
    name: String,
    contentType: ContentType,
    docStream: Source[ByteString, _])(implicit transid: TransactionId): Future[DocInfo] = ???

  override protected[core] def readAttachment[T](doc: DocInfo, name: String, sink: Sink[ByteString, Future[T]])(
    implicit transid: TransactionId): Future[(ContentType, T)] = ???

  override protected[core] def deleteAttachments[T](doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] =
    ???

  override def shutdown(): Unit = clientRef.close()

  override protected[database] def get(id: DocId)(implicit transid: TransactionId): Future[Option[JsObject]] = ???

  private def toCosmosDoc(json: JsObject): Document = {
    val computed = documentHandler.computedFields(json)
    val computedOpt = if (computed.fields.nonEmpty) Some(computed) else None
    val fieldsToAdd =
      Seq(
        (ID, Some(JsString(escapeId(json.fields(_id).convertTo[String])))),
        (E_TAG, json.fields.get(_rev)),
        (_computed, computedOpt))
    val fieldsToRemove = Seq(_id, _rev)
    val mapped = transform(json, fieldsToAdd, fieldsToRemove)
    val doc = new Document(mapped.compactPrint)
    doc.set(SELF_LINK, createSelfLink(doc.getId))
    doc
  }

  private def toWhiskJsonDoc(doc: Document): JsObject = {
    val js = doc.toJson.parseJson.asJsObject
    val fieldsToAdd = Seq((_id, Some(JsString(unescapeId(doc.getId)))), (_rev, Some(JsString(doc.getETag))))
    transform(js, fieldsToAdd, Seq.empty)
  }

  private def transform(json: JsObject, fieldsToAdd: Seq[(String, Option[JsValue])], fieldsToRemove: Seq[String]) = {
    val fields = json.fields ++ fieldsToAdd.flatMap(f => f._2.map((f._1, _))) -- fieldsToRemove
    JsObject(fields)
  }

  private def toDocInfo(doc: Document) = DocInfo(DocId(doc.getId), DocRevision(doc.getETag))

  private def createSelfLink(id: String) = s"dbs/${database.getId}/colls/${collection.getId}/docs/$id"

  private def matchRevOption(etag: String) = {
    val options = new RequestOptions
    val condition = new AccessCondition
    condition.setCondition(etag)
    options.setAccessCondition(condition)
    options
  }
}
