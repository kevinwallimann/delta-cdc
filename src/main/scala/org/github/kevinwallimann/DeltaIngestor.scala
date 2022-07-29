
package org.github.kevinwallimann

import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{max, not}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

object DeltaIngestor {
  def appendOnlyStream(df: DataFrame, partitionColumns: Seq[String], destination: String): StreamingQuery = {
    val dsw1 = df.writeStream
      .format("delta")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"${destination}/_checkpoints")
    val dsw2 = if (partitionColumns.nonEmpty) {
      dsw1.partitionBy(partitionColumns: _*)
    } else {
      dsw1
    }
    dsw2.start(destination)
  }

  def upsertStream(deltaTable: DeltaTable, df: DataFrame, destination: String, keyColumn: String, deletedExpr: Option[Column] = None): StreamingQuery = {
    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long): Unit = {
      val latestChangeForEachKey = getLatestChangeForEachKeyDF(microBatchOutputDF, keyColumn)
          val dmb1 = deltaTable.as("t")
            .merge(
              latestChangeForEachKey.as("s"),
              s"s.${keyColumn} = t.${keyColumn}"
            )
          val dmb2 = (deletedExpr match {
            case Some(expr) => dmb1.whenMatched(expr).delete
            case None => dmb1
          })
            .whenMatched()
            .updateAll()
          (deletedExpr match {
            case Some(expr) => dmb2.whenNotMatched(not(expr))
            case None => dmb2.whenNotMatched()
          })
            .insertAll()
            .execute()
    }

    def getLatestChangeForEachKeyDF(df: DataFrame, key: String) = {
      val cols = df.columns.filterNot(_ == key)
      df
        .selectExpr(key, s"struct(${cols.mkString(",")}) as otherCols" )
        .groupBy(key)
        .agg(max("otherCols").as("latest"))
        .selectExpr(key, "latest.*")
    }

    df
      .writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", s"${destination}/_checkpoints")
      .start()
  }
}
