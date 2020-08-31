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

package org.apache.spark.sql.oracle.writepath

import oracle.spark.{ConnectionManagement, ORASQLUtils}

trait WriteScenarios { self : AbstractWriteTests.type =>

  trait WriteScenario {
    val name: String
    def sql : String
    val dynPartOvwrtMode: Boolean

    def steps : Seq[WriteScenario] = Seq(this)
    def destTableSizeAfterScenario : Int
    def validateTest : Unit = ()
    def testCleanUp : Unit = ()
  }

  case class InsertScenario(name : String,
                            tableIsPart: Boolean,
                            insertOvrwt: Boolean,
                            withPartSpec: Boolean,
                            dynPartOvwrtMode: Boolean)  extends WriteScenario {

    // tableIsPart => !withPartSpec && !dynPartOvwrtMode
    assert((!tableIsPart && (!withPartSpec && !dynPartOvwrtMode)) || tableIsPart)

    // !insertOvrwt => !dynPartOvwrtMode
    assert((!insertOvrwt && !dynPartOvwrtMode) || insertOvrwt)

    lazy val pvals : (String, String) = state_channel_val.next()

    lazy val sql = {
      if (!tableIsPart) {
        val dest_tab = "unit_test_write"
        val sel_list = s"""C_CHAR_1, C_CHAR_5, C_VARCHAR2_10, C_VARCHAR2_40, C_NCHAR_1, C_NCHAR_5,
                          |C_NVARCHAR2_10, C_NVARCHAR2_40, C_BYTE, C_SHORT, C_INT, C_LONG, C_NUMBER,
                          |C_DECIMAL_SCALE_5, C_DECIMAL_SCALE_8, C_DATE, C_TIMESTAMP""".stripMargin
        val qry =
          s"""select ${sel_list}
             |from ${qual_src_tab}""".stripMargin
        val insClausePrefix = if (insertOvrwt) {
          s"insert overwrite table ${dest_tab}"
        } else {
          s"insert into ${dest_tab}"
        }

        s"""${insClausePrefix}
           |${qry}""".stripMargin

      } else {
        val dest_tab = "unit_test_write_partitioned"
        val sel_list_withoutPSpec = s"c_varchar2_40, c_int, state, channel "
        val sel_list_withPSpec = s"c_varchar2_40, c_int, channel "
        val pSpec = s"partition(state = '${pvals._1}')"
        val qrySelList = if (!withPartSpec) sel_list_withoutPSpec else sel_list_withPSpec
        val qryCond = if (!withPartSpec) "" else s"where state = '${pvals._1}'"
        val qry =
          s"""select ${qrySelList}
             |from ${qual_src_tab}
             |${qryCond}""".stripMargin

        val partClause = if (withPartSpec) pSpec else ""
        val insClausePrefix = if (insertOvrwt) {
          s"insert overwrite table ${dest_tab}"
        } else {
          s"insert into ${dest_tab}"
        }

        s"""${insClausePrefix} ${partClause}
           |${qry}""".stripMargin
      }
    }

    // scalastyle:off line.size.limit
    override def toString: String =
      s"""name=${name}, table_is_part=${tableIsPart}, insert_ovrwt=${insertOvrwt}, with_part_spec=${withPartSpec}, dynami_part_mode=${dynPartOvwrtMode}
         |SQL:
         |${sql}
         |""".stripMargin


    override def destTableSizeAfterScenario : Int = if (tableIsPart && withPartSpec) {
      srcData_PartCounts(pvals._1)
    } else 1000

    // scalastyle:on line.size.limit

    /*
     * - Check that the table/partition has the write number of rows
     * - For Write with PartSpec, check all other partitions are empty.
     */
    override def validateTest: Unit = {

      val dsKey = ConnectionManagement.getDSKeyInTestEnv
      val query = if (tableIsPart) {
        if (withPartSpec) {
          s"""
             |select count(*)
             |from sparktest.unit_test_write_partitioned
             |where state = '${pvals._1}'""".stripMargin
        } else {
          "select count(*) from sparktest.unit_test_write_partitioned"
        }
      } else {
        "select count(*) from sparktest.unit_test_write"
      }

      assert(
        ORASQLUtils.performDSQuery[Int](
          dsKey,
          query,
          s"validating Insert Scenario: '${name}''",
        ) { rs =>
          rs.next()
          rs.getInt(1)
        } == destTableSizeAfterScenario
      )

      if (tableIsPart && withPartSpec) {
        assert(
          ORASQLUtils.performDSQuery[Int](
            dsKey,
            s"""
               |select count(*)
               |from sparktest.unit_test_write_partitioned
               |where state != '${pvals._1}'""".stripMargin,
            s"validating Insert Scenario: '${name}''",
          ) { rs =>
            rs.next()
            rs.getInt(1)
          } == 0
        )
      }
    }

    override def testCleanUp : Unit = {
      val dsKey = ConnectionManagement.getDSKeyInTestEnv
      val dml = if (tableIsPart) {
        "truncate table sparktest.unit_test_write_partitioned"
      } else {
        "truncate table sparktest.unit_test_write"
      }
      ORASQLUtils.performDSDML(
        dsKey,
        dml,
        s"cleaning up Insert Scenario: '${name}''"
      )
    }

  }

  case class DeleteScenario(id : Int,
                            tableIsPart: Boolean,
                            givenPart : Option[(String, String)] = None) extends WriteScenario {
    val name : String = s"Delete Scenario ${id}"

    lazy val pvals : (String, String) = givenPart.getOrElse(state_channel_val.next())
    lazy val non_part_cbyte_check = non_part_cond_values.next()

    lazy val sql = {
      if (!tableIsPart) {
        val cond = if (non_part_cbyte_check) {
          s"c_byte < 0"
        } else {
          s"c_byte >= 0"
        }
        s"""delete from unit_test_write
           |where ${cond}""".stripMargin
      } else {
        val pCond = s"state = '${pvals._1}' and channel = '${pvals._2}'"
        s"""delete from unit_test_write_partitioned
           |where ${pCond}""".stripMargin
      }
    }

    val dynPartOvwrtMode: Boolean = false

    override def toString: String = s"""name=${name}, table_is_part=${tableIsPart}"""

    override def destTableSizeAfterScenario : Int = if (tableIsPart) {
      1000 - AbstractWriteTests.srcData_PartCounts(pvals._1)
    } else {
      if (non_part_cbyte_check) {
        499
      } else {
        501
      }
    }
  }

  case class CompositeInsertScenario(name : String,
                                     tableIsPart: Boolean,
                                     dynPartOvwrtMode: Boolean,
                                     override val steps: Seq[WriteScenario],
                                     numRowsAfterTest : Seq[WriteScenario] => Int =
                                     steps => steps.map(_.destTableSizeAfterScenario).sum
                                    ) extends WriteScenario {
    override def sql: String =
      throw new UnsupportedOperationException("No sql for a CompositeInsertScenario")

    override def validateTest: Unit = {

      val dsKey = ConnectionManagement.getDSKeyInTestEnv
      val query = if (tableIsPart) {
        "select count(*) from sparktest.unit_test_write_partitioned"
      } else {
        "select count(*) from sparktest.unit_test_write"
      }

      assert(
        ORASQLUtils.performDSQuery[Int](
          dsKey,
          query,
          s"validating Composite Insert Scenario '${name}''",
        ) { rs =>
          rs.next()
          rs.getInt(1)
        } == destTableSizeAfterScenario
      )
    }

    override def testCleanUp : Unit = {
      val dsKey = ConnectionManagement.getDSKeyInTestEnv
      val dml = if (tableIsPart) {
        "truncate table sparktest.unit_test_write_partitioned"
      } else {
        "truncate table sparktest.unit_test_write"
      }
      ORASQLUtils.performDSDML(
        dsKey,
        dml,
        s"cleaning up Insert Scenario '${name}''"
      )
    }

    override def destTableSizeAfterScenario : Int = numRowsAfterTest(steps)
  }



}
