package com.github.aalopatin

import com.github.aalopatin.utils.ConfigUtils
import com.github.aalopatin.utils.SparkUtils.unionDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object TailrecDynamicSourcesMain extends App {

  implicit val spark = SparkSession
    .builder()
    .appName("tailrec-dynamic-sources")
    .config("spark.master", "local[1]")
    .getOrCreate()

  import spark.implicits._

  val sources = ConfigUtils.getSources

  val schema = StructType(
    StructField("name", StringType, false) ::
      StructField("name", StringType, false) ::
      StructField("birthday", DateType, false) ::
      StructField("phone", StringType, false) ::
      StructField("address", StringType, false) :: Nil
  )

  val sourceDF = unionDF(sources, schema)

  val resultDF = sourceDF
    .withColumn("age", floor(months_between(current_date(), $"birthday") / 12))
    .select("source", "age")
    .groupBy("source")
    .agg(
      min("age").as("min_age"),
      max("age").as("max_age"),
      bround(avg("age"), 1).as("avg_age")
    )

  resultDF.show()

}
