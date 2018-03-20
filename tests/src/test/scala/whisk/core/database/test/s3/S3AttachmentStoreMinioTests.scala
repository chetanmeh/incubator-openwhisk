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

package whisk.core.database.test.s3

import java.io.File
import java.net.ServerSocket

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.typesafe.config.ConfigFactory
import common.{SimpleExec, WhiskProperties}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import whisk.common.TransactionId
import whisk.core.database.AttachmentStore
import whisk.core.database.s3.S3AttachmentStoreProvider
import whisk.core.database.test.AttachmentStoreBehaviors
import whisk.core.entity.WhiskEntity
import whisk.utils.retry
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class S3AttachmentStoreMinioTests extends FlatSpec with AttachmentStoreBehaviors with BeforeAndAfterAll {
  override lazy val store: AttachmentStore = {
    val config = ConfigFactory.parseString(s"""
        |whisk {
        |   s3 {
        |      alpakka {
        |         aws {
        |           credentials {
        |             provider = static
        |             access-key-id = "$accessKey"
        |             secret-access-key = "$secretAccessKey"
        |           }
        |           region {
        |             provider = static
        |             default-region = us-west-2
        |           }
        |         }
        |         endpoint-url = "http://localhost:$port"
        |      }
        |      bucket = "$bucket"
        |   }
        |}
      """.stripMargin).withFallback(ConfigFactory.load())
    S3AttachmentStoreProvider.makeStore[WhiskEntity](config)
  }

  override def storeType: String = "S3"

  override def garbageCollectAttachments: Boolean = false

  private val accessKey = "TESTKEY"
  private val secretAccessKey = "TESTSECRET"
  private val port = freePort()
  private val bucket = "test-ow-travis"

  override def afterAll(): Unit = {
    super.afterAll()
    val containerId = dockerExec("ps -q --filter ancestor=minio/minio")
    containerId.split("\n").map(_.trim).foreach(id => dockerExec(s"stop $id"))
    println(s"Stopped minio container")
  }

  def createTestBucket(): Unit = {
    val endpoint = new EndpointConfiguration(s"http://localhost:$port", "us-west-2")
    val client = AmazonS3ClientBuilder.standard
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(endpoint)
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretAccessKey)))
      .build

    retry(client.createBucket(bucket), 5, Some(1.second))
    println(s"Created bucket $bucket")
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    implicit val tid: TransactionId = transid()
    dockerExec(
      s"run -d -e MINIO_ACCESS_KEY=$accessKey -e MINIO_SECRET_KEY=$secretAccessKey -p $port:9000 minio/minio server /data")
    println(s"Started minio on $port")
    createTestBucket()
  }

  private def dockerExec(cmd: String): String = {
    implicit val tid: TransactionId = transid()
    val command = s"$dockerCmd $cmd"
    val cmdSeq = command.split(" ").map(_.trim).filter(_.nonEmpty)
    val (out, err, code) = SimpleExec.syncRunCmd(cmdSeq)
    assert(code == 0, s"Error occurred for command '$command'. Exit code: $code, Error: $err")
    out
  }

  //Taken from ActionContainer
  private lazy val dockerBin: String = {
    List("/usr/bin/docker", "/usr/local/bin/docker")
      .find { bin =>
        new File(bin).isFile
      }
      .getOrElse(???) // This fails if the docker binary couldn't be located.
  }

  private lazy val dockerCmd: String = {
    val version = WhiskProperties.getProperty("whisk.version.name")
    // Check if we are running on docker-machine env.
    val hostStr = if (version.toLowerCase().contains("mac")) {
      s" --host tcp://${WhiskProperties.getMainDockerEndpoint} "
    } else {
      ""
    }
    s"$dockerBin $hostStr"
  }

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally if (socket != null) socket.close()
  }
}
