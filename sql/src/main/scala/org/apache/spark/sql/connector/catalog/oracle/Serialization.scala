/*
  Copyright (c) 2021, Oracle and/or its affiliates.

  This software is dual-licensed to you under the Universal Permissive License
  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
  either license.

  If you elect to accept the software under the Apache License, Version 2.0,
  the following applies:

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package org.apache.spark.sql.connector.catalog.oracle

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.Kryo

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.{
  OraColumn,
  OraForeignKey,
  OraPartitionType,
  OraPrimaryKey,
  OraTable,
  OraTablePartition,
  TablePartitionScheme
}
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

class OraKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[OraTable])
    kryo.register(classOf[OraColumn])
    kryo.register(classOf[TablePartitionScheme])
    kryo.register(classOf[OraTablePartition])

    kryo.register(classOf[OraPrimaryKey])
    kryo.register(classOf[OraForeignKey])
    // kryo.register(classOf[OraPartitionType.type])
  }
}
object Serialization {

  private val BLOCKSIZE: Int = 16 * 1024

  def serialize[T: ClassTag](obj: T): Array[Byte] = {
    val ser = SparkEnv.get.serializer
    val out = new ChunkedByteBufferOutputStream(BLOCKSIZE, ByteBuffer.allocate)
    val serInst = ser.newInstance()
    val serOut = serInst.serializeStream(out)
    Utils.tryWithSafeFinally {
      serOut.writeObject[T](obj)
    } {
      serOut.close()
    }
    out.toChunkedByteBuffer.toArray
  }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T = {
    val serializer = SparkEnv.get.serializer
    val in = new ByteArrayInputStream(bytes)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj: T = Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
    obj
  }

}
