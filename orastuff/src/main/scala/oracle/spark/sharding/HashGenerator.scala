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

package oracle.spark.sharding

/*
 Mimic the behavior of 'oracle.jdbc.pool.KggHashGenerator.hash' function.
 */
object HashGenerator {

  def hash(arrby: Array[Byte]): Int = hash(arrby, 0, arrby.length, 0)

  def hash(var0: Array[Byte], var1: Int, var2: Int, var3: Int) : Int = {
    val var4 : Int = var2 - var1
    var var5 : Int = var4
    var var6 : Int = var1
    var var7 : Int = -1640531527
    var var8 : Int = -1640531527
    var var9 : Int = -1640531527
    var var10 : Int = 0

    var10 = var3
    while (var5 >= 0) {
      var var11 : Int = 0
      var var12 : Int = 0
      var var13 : Int = 0
      var var14 : Int = 0
      val var15 = var5 & -16
      val var16 = if (var15 != 0) 16 else var5 & 15

      if (var16 >= 16) {
        var14 |= (var0(var6 + 15) & 255) << 24
      }

      if (var16 >= 15) {
        var14 |= (var0(var6 + 14) & 255) << 16
      }

      if (var16 >= 14) {
        var14 |= (var0(var6 + 13) & 255) << 8
      }

      if (var16 >= 13) {
        var14 |= var0(var6 + 12) & 255
      }

      if (var16 >= 12) {
        var13 |= (var0(var6 + 11) & 255) << 24
      }

      if (var16 >= 11) {
        var13 |= (var0(var6 + 10) & 255) << 16
      }

      if (var16 >= 10) {
        var13 |= (var0(var6 + 9) & 255) << 8
      }

      if (var16 >= 9) {
        var13 |= var0(var6 + 8) & 255
      }

      if (var16 >= 8) {
        var12 |= (var0(var6 + 7) & 255) << 24
      }

      if (var16 >= 7) {
        var12 |= (var0(var6 + 6) & 255) << 16
      }

      if (var16 >= 6) {
        var12 |= (var0(var6 + 5) & 255) << 8
      }

      if (var16 >= 5) {
        var12 |= var0(var6 + 4) & 255
      }

      if (var16 >= 4) {
        var11 |= (var0(var6 + 3) & 255) << 24
      }

      if (var16 >= 3) {
        var11 |= (var0(var6 + 2) & 255) << 16
      }

      if (var16 >= 2) {
        var11 |= (var0(var6 + 1) & 255) << 8
      }

      if (var16 >= 1) {
        var11 |= var0(var6 + 0) & 255
      }

      var10 += (if (var15 == 0) var4 + (var14 << 8) else var14)
      var7 += var11
      var8 += var12
      var9 += var13
      var7 += var10
      var10 += var7
      var7 ^= var7 >>> 7
      var8 += var7
      var7 += var8
      var8 ^= var8 << 13
      var9 += var8
      var8 += var9
      var9 ^= var9 >>> 17
      var10 += var9
      var9 += var10
      var10 ^= var10 << 9
      var7 += var10
      var10 += var7
      var7 ^= var7 >>> 3
      var8 += var7
      var7 += var8
      var8 ^= var8 << 7
      var9 += var8
      var8 += var9
      var9 ^= var9 >>> 15
      var10 += var9
      var9 += var10
      var10 ^= var10 << 11
      var6 += 16

      var5 -= 16
    }
    var10
  }

}
