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

import akka.stream.ActorMaterializer
import common.WskActorSystem
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}
import whisk.common.TransactionId
import whisk.core.database.AttachmentStore
import whisk.core.database.couchdb.CouchDbAttachmentStoreProvider
import whisk.core.entity._
import whisk.core.entity.test.ExecHelpers

import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class CouchDbAttachmentStoreTests
    extends FlatSpec
    with AttachmentStoreBehaviors
    with WskActorSystem
    with DbUtils
    with ExecHelpers
    with BeforeAndAfter
    with BeforeAndAfterAll {
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  private lazy val entityStore = WhiskEntityStore.datastore()
  override val store: AttachmentStore = CouchDbAttachmentStoreProvider.makeStore[WhiskEntity]()
  override def storeType: String = "CouchDb"

  override protected def newDocInfo: DocInfo = {
    //Seed in a dummy instance so that attachment can be attached
    implicit val tid: TransactionId = transid()
    val trigger = WhiskTrigger(EntityPath("test namespace"), EntityName(newDocId))
    put(entityStore, trigger)
  }

  override protected def deleteAttachment(info: DocInfo)(implicit transid: TransactionId): Future[Boolean] =
    entityStore.del(info)

  override protected def garbageCollect(doc: DocInfo): Unit = {
    docsToDelete += ((entityStore, doc))
  }

  after {
    cleanup()
  }

  override def afterAll(): Unit = {
    entityStore.shutdown()
    store.shutdown()
    super.afterAll()
  }
}
