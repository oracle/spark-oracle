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

package org.apache.spark.sql.connector.catalog.oracle.sharding

import scala.collection.mutable.ArrayBuffer

import oracle.spark.ConnectionManagement
import oracle.spark.ORASQLUtils

import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.catalog.oracle.OracleCatalog
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.ShardingAbstractTest
import org.apache.spark.sql.types.Decimal


class ShardingMetadataTest extends ShardingAbstractTest {

  private val ordersTab = QualifiedTableName("TPCH", "ORDERS")
  private lazy val shardMD = OracleCatalog.oracleCatalog.getMetadataManager.getShardingMetadata
  private lazy val shardInstances = shardMD.shardInstances
  private lazy val tableFamily = shardMD.rootTblFamilyMap(ordersTab)
  private lazy val routingTab = shardMD.routingTables(tableFamily.id)

  private def orderKeysList(shardInstance: ShardInstance): Seq[Literal] = {
    def orderKeyLiteral(l: Long): Literal = {
      val dec = Decimal(new java.math.BigDecimal(l), 38, 18)
      Literal(dec)
    }

    val order_keys_query =
      """select o_orderkey from orders sample(1) where rownum < 1000"""

    val oLits = ArrayBuffer[Literal]()
    val shardDSKey = ConnectionManagement.registerDataSource(
      shardInstance.shardDSInfo.connInfo,
      shardInstance.shardDSInfo.catalogOptions)
    ORASQLUtils.performDSQuery(
      shardDSKey,
      order_keys_query,
      "validate order key presence in shard instance",
    ) { rs =>
      while (rs.next()) {
        oLits += orderKeyLiteral(rs.getLong(1))
      }
      oLits
    }
  }

  test("showTablesAndDescribe") { td =>
    TestOracleHive.sql("show tables").show(1000, false)
    TestOracleHive.sql("describe orders").show(1000, false)
    TestOracleHive.sql("describe lineitem").show(1000, false)
  }

  test("routing") { _ =>
    for (shardInst <- shardInstances) {
      val keys = orderKeysList(shardInst)
      for (key <- keys) {
        val shards = routingTab.lookupShardsEQ(key)
        val shard = shardMD.shardInstances(shards.head)
        assert(shards.size == 1 && shard.connectString == shardInst.connectString)
      }
    }
  }

  test("showShardInstances") { td =>
    TestOracleHive.sql("show shard_instances").show(1000, false)
  }

  test("showTableFamilies") { td =>
    TestOracleHive.sql("show table_families").show(1000, false)
  }

  test("showReplicatedTables") { td =>
    TestOracleHive.sql("show replicated tables").show(1000, false)
  }

  test("showShardedTables") { td =>
    TestOracleHive.sql("show sharded tables").show(1000, false)
  }

  test("showRoutingTable") { td =>
    TestOracleHive.sql("show routing_table tpch.orders").show(1000, false)
  }
}
