package com.github.aalopatin

import com.github.aalopatin.Columns.{ColumnsAll, CompositeKey, Product, Rate, TimeBucket, ValidFrom}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, date_add, date_sub, explode, expr, last, lead, lit, min, sequence, to_date}
import org.apache.spark.sql.types.{DateType, FloatType, StringType, StructField, StructType}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object FillMissingDatesMain extends App {
  implicit val spark = SparkSession
    .builder()
    .appName("fill-missing-dates")
    .config("spark.master", "local[*]")
    .getOrCreate()

  import spark.implicits._

  val schema = StructType(
    StructField(Product, StringType, nullable = false) ::
      StructField(TimeBucket, StringType, nullable = false) ::
      StructField(ValidFrom, DateType, nullable = false) ::
      StructField(Rate, FloatType, nullable = false) :: Nil
  )

  val currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

  val dataDF =
    spark
      .read
      .option("multiline", "true")
      .schema(schema)
      .json("fill-missing-dates/src/main/resources/data/rates.json")
      .where($"$ValidFrom" <= currentDate)
      .orderBy((CompositeKey :+ ValidFrom).map(col):_*)
      .select(ColumnsAll.map(col):_*)

  dataDF.show(250)

  val windowKeyByValidFrom = Window.partitionBy(CompositeKey.map(col):_*).orderBy(ValidFrom)

  // Last
  val allDatesDF =
    dataDF
      .groupBy(CompositeKey.map(col):_*)
      .agg(
        min(ValidFrom).as("min_date")
      )
      .withColumn("max_date", to_date(lit(currentDate)))
      .withColumn(ValidFrom, sequence($"min_date", $"max_date", expr("interval 1 day")))
      .withColumn(ValidFrom, explode($"$ValidFrom"))
      .drop("min_date", "max_date")

  val resultLastDF =
    allDatesDF
      .join(
        dataDF,
        CompositeKey :+ ValidFrom,
        "left"
      )
      .withColumn(Rate, last(Rate, ignoreNulls = true).over(windowKeyByValidFrom))
      .orderBy((CompositeKey :+ ValidFrom).map(col):_*)

  resultLastDF
    .write
    .mode(SaveMode.Overwrite)
    .json("fill-missing-dates/src/main/resources/data/last.json")

  // Explode
  val resultExplodeDF =
    dataDF
      .withColumn("min_date", $"$ValidFrom")
      .withColumn("max_date", lead(date_sub($"$ValidFrom", 1), 1, currentDate).over(windowKeyByValidFrom))
      .withColumn(ValidFrom, sequence($"min_date", $"max_date", expr("interval 1 day")))
      .withColumn(ValidFrom, explode($"$ValidFrom"))
      .select(ColumnsAll.map(col):_*)

  resultExplodeDF
    .write
    .mode(SaveMode.Overwrite)
    .json("fill-missing-dates/src/main/resources/data/explode.json")

}
