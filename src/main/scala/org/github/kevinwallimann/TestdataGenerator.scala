
package org.github.kevinwallimann

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

import java.sql.Date
import java.time.temporal.ChronoUnit

object TestdataGenerator {

  def insertOnlyData(sizePerDay: Int, startDate: Date, endDate: Date, valuesFn: Int => String = i => s"Hero_#$i"): (StructType, Seq[Row]) = {
    val schema = new StructType()
      .add("key", IntegerType, nullable = false)
      .add("value", StringType, nullable = false)
      .add("info_date", DateType, nullable = false)

    val days = ChronoUnit.DAYS.between(startDate.toLocalDate, endDate.toLocalDate).toInt
    val rows = (1 to sizePerDay * days).map { i =>
      val currentDay = Date.valueOf(startDate.toLocalDate.plusDays(i / sizePerDay))
      Row(i, valuesFn(i), currentDay)
    }

    (schema, rows)
  }
}
