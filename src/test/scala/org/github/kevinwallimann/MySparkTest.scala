package org.github.kevinwallimann

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType, TimestampType}
import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.commons.spark.SparkTestBase

class MySparkTest extends AnyFlatSpec with SparkTestBase {
  behavior of "Spark"

  // TODO: Implement generator of data

//  to_timestamp(lit("2022-10-10 02:00:00"), "yyyy-MM-dd HH:mm:ss")
//  to_timestamp(lit("2022-10-10 02:00:00"), "yyyy-MM-dd HH:mm:ss")
//  to_timestamp(lit("2022-10-10 02:00:00"), "yyyy-MM-dd HH:mm:ss")
//  to_timestamp(lit("2022-10-10 02:00:00"), "yyyy-MM-dd HH:mm:ss")
//  to_timestamp(lit("2022-10-10 01:00:00"), "yyyy-MM-dd HH:mm:ss")
//  to_timestamp(lit("2022-10-10 01:00:00"), "yyyy-MM-dd HH:mm:ss")
//  to_timestamp(lit("2022-10-10 01:00:00"), "yyyy-MM-dd HH:mm:ss")
//  to_timestamp(lit("2022-10-10 01:00:00"), "yyyy-MM-dd HH:mm:ss")
//  to_timestamp(lit("2022-10-10 01:00:00"), "yyyy-MM-dd HH:mm:ss")

  it should "ingest the rows" in {
    import spark.implicits._
    val schemaCatalyst = new StructType()
      .add("mode", StringType, nullable = false)
      .add("key", IntegerType, nullable = false)
      .add("time", IntegerType, nullable = false)
      .add("value", StringType, nullable = false)
      .add("group", IntegerType, nullable = false)
    val rows = Seq(
      Row("U", 1, 6, "A02", 1),
      Row("U", 2, 7, "B02", 1),
      Row("I", 6, 8, "F02", 2),
      Row("I", 7, 9, "G02", 2),
      Row("I", 1, 1, "A01", 1),
      Row("I", 2, 2, "B01", 1),
      Row("I", 3, 3, "C01", 2),
      Row("I", 4, 4, "D01", 2),
      Row("I", 5, 5, "E01", 2),
    )

    val input = MemoryStream[Row](42, spark.sqlContext)(RowEncoder(schemaCatalyst))

    val path = "/tmp/delta-cdc"
    val deltaTableOpt = if (DeltaTable.isDeltaTable(path)) {
      Some(DeltaTable.forPath(path))
    } else {
      None
    }

    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long): Unit = {
      val latestChangeForEachKey = getLatestChangeForEachKeyDF(microBatchOutputDF)
      deltaTableOpt match {
        case Some(deltaTable) =>
          deltaTable.as("t")
            .merge(
              latestChangeForEachKey.as("s"),
              "s.key = t.key")
            .whenMatched("s.mode = 'D'")
            .delete()
            .whenMatched()
            .updateExpr(Map("key" -> "s.key", "value" -> "s.value"))
            .whenNotMatched("s.mode <> 'D'")
            .insertExpr(Map("key" -> "s.key", "value" -> "s.value"))
            .execute()
        case None =>
          latestChangeForEachKey.
          write.
          partitionBy("group").
          format("delta").
          save("/tmp/delta-cdc")
      }

    }

    def getLatestChangeForEachKeyDF(df: DataFrame) = {
      df
        .selectExpr("key", "struct(time, value, mode, group) as otherCols" )
        .groupBy("key")
        .agg(max("otherCols").as("latest"))
        .selectExpr("key", "latest.*")
    }

    val df = input.toDF().
          writeStream.
          format("delta").
          foreachBatch(upsertToDelta _).
          outputMode(OutputMode.Update()).
          option("checkpointLocation", s"${path}/_checkpoints")


    val query = df.start()
    input.addData(rows)
    query.processAllAvailable()
  }
}