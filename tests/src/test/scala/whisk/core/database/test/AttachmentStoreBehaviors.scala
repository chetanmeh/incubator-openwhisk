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

package whisk.core.database.test

import java.io.ByteArrayInputStream

import akka.http.scaladsl.model.ContentTypes
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.{ByteString, ByteStringBuilder}
import common.StreamLogging
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FlatSpec, Matchers}
import whisk.common.{TransactionCounter, TransactionId}
import whisk.core.database.{AttachmentStore, NoDocumentException}
import whisk.core.entity.DocInfo

import scala.concurrent.Future
import scala.util.Random

trait AttachmentStoreBehaviors
    extends ScalaFutures
    with TransactionCounter
    with Matchers
    with StreamLogging
    with IntegrationPatience {
  this: FlatSpec =>

  override val instanceOrdinal = 0

  private val prefix = Random.alphanumeric.take(10).mkString

  def store: AttachmentStore

  def storeType: String

  behavior of s"$storeType AttachmentStore"

  it should "add and read attachment" in {
    implicit val tid: TransactionId = transid()
    val bytes = randomBytes(4000)

    val info = newDocInfo
    val info_v2 = store.attach(info, "code", ContentTypes.`application/octet-stream`, chunkedSource(bytes)).futureValue

    info_v2.id shouldBe info.id

    val (contentType, byteBuilder) = store.readAttachment(info_v2, "code", byteStringSink).futureValue

    contentType shouldBe ContentTypes.`application/octet-stream`
    byteBuilder.result() shouldBe ByteString(bytes)
    garbageCollect(info_v2)
  }

  it should "add and then update attachment" in {
    implicit val tid: TransactionId = transid()
    val bytes = randomBytes(4000)

    val info = newDocInfo
    val info_v2 = store.attach(info, "code", ContentTypes.`application/octet-stream`, chunkedSource(bytes)).futureValue

    info_v2.id shouldBe info.id

    val updatedBytes = randomBytes(7000)
    val info_v3 =
      store.attach(info_v2, "code", ContentTypes.`application/json`, chunkedSource(updatedBytes)).futureValue

    info_v3.id shouldBe info.id

    val (contentType, byteBuilder) = store.readAttachment(info_v3, "code", byteStringSink).futureValue

    contentType shouldBe ContentTypes.`application/json`
    byteBuilder.result() shouldBe ByteString(updatedBytes)

    garbageCollect(info_v3)
  }

  it should "add and delete attachment" in {
    implicit val tid: TransactionId = transid()
    val bytes = randomBytes(4000)

    val info = newDocInfo
    val info_v2 = store.attach(info, "code", ContentTypes.`application/octet-stream`, chunkedSource(bytes)).futureValue
    val info_v3 = store.attach(info_v2, "code2", ContentTypes.`application/json`, chunkedSource(bytes)).futureValue

    val info2 = newDocInfo
    val info2_v2 = store.attach(info2, "code2", ContentTypes.`application/json`, chunkedSource(bytes)).futureValue

    info_v2.id shouldBe info.id
    info_v3.id shouldBe info.id
    info2_v2.id shouldBe info2.id

    def getAttachmentType(info: DocInfo, name: String) = {
      store.readAttachment(info, name, byteStringSink)
    }

    getAttachmentType(info_v3, "code").futureValue._1 shouldBe ContentTypes.`application/octet-stream`
    getAttachmentType(info_v3, "code2").futureValue._1 shouldBe ContentTypes.`application/json`

    val deleteResult = deleteAttachment(info_v3)

    deleteResult.futureValue shouldBe true

    getAttachmentType(info_v3, "code").failed.futureValue shouldBe a[NoDocumentException]
    getAttachmentType(info_v3, "code2").failed.futureValue shouldBe a[NoDocumentException]

    //Delete should not have deleted other attachments
    getAttachmentType(info2_v2, "code2").futureValue._1 shouldBe ContentTypes.`application/json`
    garbageCollect(info2_v2)
  }

  it should "throw NoDocumentException on reading non existing attachment" in {
    implicit val tid: TransactionId = transid()

    val info = DocInfo ! ("nonExistingAction", "1")
    val f = store.readAttachment(info, "code", byteStringSink)

    f.failed.futureValue shouldBe a[NoDocumentException]
  }

  it should "not write an attachment when there is error in Source" in {
    implicit val tid: TransactionId = transid()

    val info = newDocInfo
    val error = new Error("boom!")
    val faultySource = Source(1 to 10)
      .map { n ⇒
        if (n == 7) throw error
        n
      }
      .map(ByteString(_))
    val writeResult = store.attach(info, "code", ContentTypes.`application/octet-stream`, faultySource)
    writeResult.failed.futureValue.getCause should be theSameInstanceAs error

    val readResult = store.readAttachment(info, "code", byteStringSink)
    readResult.failed.futureValue shouldBe a[NoDocumentException]
  }

  it should "throw exception when doc is null" is pending
  it should "have start and end markers" is pending

  protected def deleteAttachment(info: DocInfo)(implicit transid: TransactionId): Future[Boolean] = {
    store.deleteAttachments(info)
  }

  protected def newDocInfo: DocInfo = {
    //By default create an info with dummy revision
    //as apart from CouchDB other stores do not support the revision property
    //for blobs
    DocInfo ! (newDocId, "1")
  }

  protected def garbageCollect(doc: DocInfo): Unit = {}

  @volatile private var counter = 0
  protected def newDocId: String = {
    counter = counter + 1
    s"attachmentTests_${prefix}_$counter"
  }

  private def randomBytes(size: Int): Array[Byte] = {
    val arr = new Array[Byte](size)
    Random.nextBytes(arr)
    arr
  }

  private def chunkedSource(bytes: Array[Byte]): Source[ByteString, _] = {
    StreamConverters.fromInputStream(() => new ByteArrayInputStream(bytes), 42)
  }

  private def byteStringSink = {
    Sink.fold[ByteStringBuilder, ByteString](new ByteStringBuilder)((builder, b) => builder ++= b)
  }
}
