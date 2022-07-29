package org.github.kevinwallimann

import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.commons.spark.SparkTestBase

import java.sql.Date

class MySparkTest extends AnyFlatSpec with SparkTestBase {
  behavior of "Spark"

  it should "append only" in {
    val data = TestdataGenerator.insertOnlyData(100, Date.valueOf("2022-07-26"), Date.valueOf("2022-07-30"))
    val schema = data._1
    val rows = data._2
    val size = rows.size / 5
    val rowsSplit = rows.sliding(size, size)

    val input = MemoryStream[Row](42, spark.sqlContext)(RowEncoder(schema))

    val path = "/tmp/delta-cdc/append-only"

    val query = DeltaIngestor.appendOnlyStream(input.toDF(), Seq("info_date"), path)

    rowsSplit.foreach { currentRows =>
      input.addData(currentRows)
      query.processAllAvailable()
    }
  }

  it should "upsert the rows" in {
    def executeQuery(df: DataFrame) = {
      val path = "/tmp/delta-cdc"
      val query = if (DeltaTable.isDeltaTable(path)) {
        DeltaIngestor.upsertStream(DeltaTable.forPath(path), df, path, "key")
      } else {
        DeltaIngestor.appendOnlyStream(df, Seq("info_date"), path)
      }
      query.processAllAvailable()
    }

    val data1 = TestdataGenerator.insertOnlyData(100, Date.valueOf("2022-07-26"), Date.valueOf("2022-07-30"))
    val schema = data1._1
    val input = MemoryStream[Row](42, spark.sqlContext)(RowEncoder(schema))
    input.addData(data1._2)
    executeQuery(input.toDF())

    val data1Fix = TestdataGenerator.insertOnlyData(100, Date.valueOf("2022-07-26"), Date.valueOf("2022-08-02"), i => if (i == 42) "The Master Hero" else s"Hero_#$i")
    input.addData(data1Fix._2)
    executeQuery(input.toDF())

    // scala> val df = spark.read.format("delta").load("/tmp/delta-cdc")
    // scala> df.where("key==42").show
    // +---+---------------+----------+
    // |key|          value| info_date|
    // +---+---------------+----------+
    // | 42|The Master Hero|2022-07-26|
    // +---+---------------+----------+
    // scala> val df0 = spark.read.format("delta").option("versionAsOf","0").load("/tmp/delta-cdc")
    // scala> df0.where("key==42").show
    // +---+--------+----------+
    // |key|   value| info_date|
    // +---+--------+----------+
    // | 42|Hero_#42|2022-07-26|
    // +---+--------+----------+
  }
}