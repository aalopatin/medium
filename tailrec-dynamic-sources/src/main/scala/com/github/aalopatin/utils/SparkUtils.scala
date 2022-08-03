package com.github.aalopatin
package utils

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.annotation.tailrec

object SparkUtils {

  def unionDF(sources: Map[String, String], schema: StructType)
             (implicit spark: SparkSession): DataFrame = {

    val cols = schema.fieldNames.toSeq.map(col)

    val emptyDF = spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      schema.add("source", StringType, false)
    )

    union(sources.iterator, cols, emptyDF)

  }

  @tailrec
  private def union(sources: Iterator[(String, String)], cols: Seq[Column], accumulator: DataFrame)
           (implicit spark: SparkSession): DataFrame = {

    if (!sources.hasNext) accumulator
    else {
      val source = sources.next()
      val accum =
        accumulator
          .union(
            readTableDF(source._2)
              .select(cols: _*)
              .withColumn("source", lit(source._1))
          )
      union(sources, cols, accum)

    }
  }

  private def readTableDF(dbTable: String)
                 (implicit spark: SparkSession) = {
    spark.read
      .format("jdbc")
      .options(ConfigUtils.getJDBC)
      .option("dbtable", dbTable)
      .load()
  }

}
