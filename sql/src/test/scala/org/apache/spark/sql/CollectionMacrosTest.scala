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
package org.apache.spark.sql

import org.apache.spark.unsafe.types.CalendarInterval

class CollectionMacrosTest extends MacrosAbstractTest {

  import org.apache.spark.sql.catalyst.ScalaReflection._
  import universe._

  test("arrFuncs") { td =>

    handleMacroOutput(eval(reify {(s : String) =>
      Array(s).size
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      Array(s).zip[String, String, Array[(String, String)]](Array(s))
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      (Array(s, s).mkString, Array(s, s).mkString(", "))
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      (Array(s, s).min[String], Array(s, s).max[String])
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      Array(s, s).slice(0, 2).reverse ++[String, Array[String]] Array(s, s)
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      (Array(Array(s, s), Array(s, s)).flatten[String] intersect[String] Array(s, s)).distinct
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      Array.fill[String](5)(s)
    }.tree)
    )
  }

  test("collectionUtilFuncs") { td =>
    import org.apache.spark.sql.sqlmacros.CollectionUtils._

    handleMacroOutput(eval(reify {(s : String) =>
      mapFromEntries(mapEntries(Map(s -> s)))
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      overlapArrays(
        sortArray(shuffleArray(Array(s, s)), true),
        Array(s, s)
      )
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      positionArray(Array(s, s), s)
    }.tree)
    )

    handleMacroOutput(eval(reify {(start : Int, stop : Int) =>
      sequence(start, stop, 1)
    }.tree)
    )

    handleMacroOutput(eval(reify {(start : java.sql.Date, stop : java.sql.Date) =>
      date_sequence(start, stop, new CalendarInterval(0,0, 1000L))
    }.tree)
    )

    handleMacroOutput(eval(reify {(start : java.sql.Timestamp, stop : java.sql.Timestamp) =>
      timestamp_sequence(start, stop, new CalendarInterval(0,0, 1000L))
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      exceptArray(removeArray(Array(s, s), s), Array(s))
    }.tree)
    )

    handleMacroOutput(eval(reify {(s : String) =>
      mapKeys(Map(s -> s) ++ Map(s -> s)) ++[String, Array[String]] mapValues(Map(s -> s))
    }.tree)
    )
  }

}
