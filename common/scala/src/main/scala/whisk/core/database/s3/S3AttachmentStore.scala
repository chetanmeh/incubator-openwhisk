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

package whisk.core.database.s3

import akka.actor.ActorSystem
import akka.event.Logging.ErrorLevel
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpHeader}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.impl.MetaHeaders
import akka.stream.alpakka.s3.scaladsl.{ObjectMetadata, S3Client}
import akka.stream.alpakka.s3.{S3Exception, S3Settings}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.typesafe.config.Config
import pureconfig.loadConfigOrThrow
import whisk.common.LoggingMarkers.{DATABASE_ATT_DELETE, DATABASE_ATT_GET, DATABASE_ATT_SAVE}
import whisk.common.{Logging, StartMarker, TransactionId}
import whisk.core.ConfigKeys
import whisk.core.database._
import whisk.core.entity.DocInfo

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

object S3AttachmentStoreProvider extends AttachmentStoreProvider {
  val alpakkaConfigKey = s"${ConfigKeys.s3}.alpakka"
  case class S3Config(bucket: String) {
    def prefixFor[D](implicit tag: ClassTag[D]): String = {
      tag.runtimeClass.getSimpleName.toLowerCase
    }
  }

  override def makeStore[D <: DocumentSerializer: ClassTag]()(implicit actorSystem: ActorSystem,
                                                              logging: Logging,
                                                              materializer: ActorMaterializer): AttachmentStore = {
    val client = new S3Client(S3Settings(alpakkaConfigKey))
    val config = loadConfigOrThrow[S3Config](ConfigKeys.s3)
    new S3AttachmentStore(client, config.bucket, config.prefixFor[D])
  }

  def makeStore[D <: DocumentSerializer: ClassTag](config: Config)(implicit actorSystem: ActorSystem,
                                                                   logging: Logging,
                                                                   materializer: ActorMaterializer): AttachmentStore = {
    val client = new S3Client(S3Settings(config, alpakkaConfigKey))
    val s3config = loadConfigOrThrow[S3Config](config, ConfigKeys.s3)
    new S3AttachmentStore(client, s3config.bucket, s3config.prefixFor[D])
  }

}
class S3AttachmentStore(client: S3Client, bucket: String, prefix: String)(implicit system: ActorSystem,
                                                                          logging: Logging,
                                                                          materializer: ActorMaterializer)
    extends AttachmentStore {
  private val metaContentType = "content-type"
  private val amzMetaContentType = s"x-amz-meta-$metaContentType"

  override protected[core] implicit val executionContext: ExecutionContext = system.dispatcher

  override protected[core] def attach(
    doc: DocInfo,
    name: String,
    contentType: ContentType,
    docStream: Source[ByteString, _])(implicit transid: TransactionId): Future[DocInfo] = {
    checkDocState(doc)
    val start = transid.started(this, DATABASE_ATT_SAVE, s"[ATT_PUT] uploading attachment '$name' of document '$doc'")

    //Due to bug in alpakka the ContentType header is not accessible
    //in download flow. To workaround that the contentType is stored
    //as meta property
    //https://github.com/akka/alpakka/issues/853

    //A possible optimization for small attachments < 5MB can be to use putObject instead of multipartUpload
    //and thus use 1 remote call instead of 3
    val f = docStream
      .runWith(
        client.multipartUpload(
          bucket,
          objectKey(doc, name),
          contentType,
          MetaHeaders(Map(metaContentType -> contentType.toString()))))
      .map(_ => doc)

    f.onSuccess({
      case _ =>
        transid
          .finished(this, start, s"[ATT_PUT] '$prefix' completed uploading attachment '$name' of document '$doc'")
    })

    reportFailure(
      f,
      start,
      failure => s"[ATT_PUT] '$prefix' internal error, name: '$name', doc: '$doc', failure: '${failure.getMessage}'")
  }

  override protected[core] def readAttachment[T](doc: DocInfo, name: String, sink: Sink[ByteString, Future[T]])(
    implicit transid: TransactionId): Future[(ContentType, T)] = {
    checkDocState(doc)
    require(name != null, "name undefined")
    val start =
      transid.started(this, DATABASE_ATT_GET, s"[ATT_GET] '$prefix' finding attachment '$name' of document '$doc'")
    val (source, metaFuture) = client.download(bucket, objectKey(doc, name))

    val bodyFuture = source.runWith(sink)
    val f = for {
      body <- bodyFuture
      meta <- metaFuture
    } yield (getContentType(meta), body)

    val g = f.transform(
      { s =>
        transid.finished(this, start, s"[ATT_GET] '$prefix' completed: found attachment '$name' of document '$doc'")
        s
      }, {
        case s: S3Exception if s.code == "NoSuchKey" =>
          transid
            .finished(this, start, s"[ATT_GET] '$prefix', retrieving attachment '$name' of document '$doc'; not found.")
          NoDocumentException("Not found on 'readAttachment'.")
        case e => e
      })

    reportFailure(
      g,
      start,
      failure => s"[ATT_GET] '$prefix' internal error, name: '$name', doc: '$doc', failure: '${failure.getMessage}'")
  }

  override protected[core] def deleteAttachments(doc: DocInfo)(implicit transid: TransactionId): Future[Boolean] = {
    val start = transid.started(this, DATABASE_ATT_DELETE, s"[ATT_DELETE] uploading attachment of document '$doc'")

    //S3 provides API to delete multiple objects in single call however alpakka client
    //currently does not support that and also in current usage 1 docs has at most 1 attachment
    //so current approach would also involve 2 remote calls
    val f = client
      .listBucket(bucket, Some(objectKeyPrefix(doc)))
      .mapAsync(1)(bc => client.deleteObject(bc.bucketName, bc.key))
      .runWith(Sink.seq)
      .map(_ => true)

    f.onSuccess {
      case _ =>
        transid.finished(this, start, s"[ATT_DELETE] completed: delete attachment of document '$doc'")
    }

    reportFailure(
      f,
      start,
      failure => s"[ATT_DELETE] '$prefix' internal error, doc: '$doc', failure: '${failure.getMessage}'")
  }

  override def shutdown(): Unit = {}

  private def getContentType(meta: ObjectMetadata): ContentType = {
    meta.contentType match {
      case Some(typeString) => getContentType(typeString)
      case None =>
        meta.metadata
          .collectFirst {
            case HttpHeader(`amzMetaContentType`, typeString) => getContentType(typeString)
          }
          .getOrElse(ContentTypes.NoContentType)
    }
  }

  private def getContentType(contentType: String): ContentType = {
    ContentType.parse(contentType) match {
      case Right(ct) => ct
      case Left(_)   => ContentTypes.NoContentType //Should not happen
    }
  }

  private def objectKey(doc: DocInfo, name: String): String = {
    s"$prefix/${doc.id.id}/$name"
  }

  private def objectKeyPrefix(doc: DocInfo): String = {
    s"$prefix/${doc.id.id}"
  }

  private def checkDocState(doc: DocInfo): Unit = {
    require(doc != null, "doc undefined")
    require(doc.rev.rev != null, "doc revision must be specified")
  }

  private def reportFailure[T](f: Future[T], start: StartMarker, failureMessage: Throwable => String)(
    implicit transid: TransactionId): Future[T] = {
    f.onFailure({
      case _: ArtifactStoreException => // These failures are intentional and shouldn't trigger the catcher.
      case x                         => transid.failed(this, start, failureMessage(x), ErrorLevel)
    })
    f
  }
}
