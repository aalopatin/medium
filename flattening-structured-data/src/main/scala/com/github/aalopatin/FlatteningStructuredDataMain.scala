package com.github.aalopatin

import com.github.aalopatin.implicits.Helpers.DataFrameHelper
import org.apache.spark.sql.functions.{col, explode, first, map_from_arrays, map_keys}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlatteningStructuredDataMain extends App {

  implicit val spark = SparkSession
    .builder()
    .appName("flattening-structured-data")
    .config("spark.master", "local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

//  1. Data with array
  val carsDF_data_array = spark
    .read
    .option("multiline","true")
    .json("data/cars/data-array.json")
    .select("manufacturer", "model", "color")

  carsDF_data_array.show(10, false)
  carsDF_data_array.printSchema()
  carsDF_data_array.withColumn("color", explode($"color")).show(false)
//  carsDF_data_array.flatten().show(false)

////  2. Data with objects
//  val carsDF_data_objects = spark
//    .read
//    .option("multiline","true")
//    .json("data/cars/data-object.json")
//
//  carsDF_data_objects.show(false)
//  carsDF_data_objects.flatten().show(false)
//
////  3. Complex Data
//  val carsDF_data = spark
//    .read
//    .option("multiline","true")
//    .json("data/cars/data.json")
//
//  carsDF_data.show(false)
//  carsDF_data.flatten().show(false)
//
//
////  4. Complex Data with kay-value objects - pivot
//  val pivot_properties = (df: DataFrame) => {
//    val df_exploded = df
//      .withColumn("properties_exploded", explode($"properties"))
//      .drop("properties")
//
//    val cols = df_exploded.schema.fieldNames.filterNot(field => field == "properties_exploded" )
//    df_exploded.groupBy(cols.map(col):_*).pivot("properties_exploded.key").agg(first("properties_exploded.value"))
//  }
//
//  val carsDF_data_object_key_value_pivot = spark
//    .read
//    .option("multiline","true")
//    .json("data/cars/data-object-key-value.json")
//
//  carsDF_data_object_key_value_pivot.show(false)
//  carsDF_data_object_key_value_pivot.flatten(Map("properties" -> pivot_properties)).show(false)
//
////  5. Complex Data with kay-value objects - collect
//  val map_properties_collect = (df: DataFrame) => {
//    val mappedDF = df
//      .withColumn("properties_map", map_from_arrays($"properties.key", $"properties.value"))
//      .drop("properties")
//
//    val keysDF = mappedDF.select(explode(map_keys($"properties_map"))).distinct()
//    val keys = keysDF.collect().map(f=>f.get(0))
//    val keyCols = keys.map(f=> col("properties_map").getItem(f).as(f.toString))
//    mappedDF.select(col("*") +: keyCols:_*).drop("properties_map")
//  }
//
//  val carsDF_data_object_key_value_collect = spark
//    .read
//    .option("multiline","true")
//    .json("data/cars/data-object-key-value.json")
//
//  carsDF_data_object_key_value_collect.show(false)
//  carsDF_data_object_key_value_collect.flatten(Map("properties" -> map_properties_collect)).show(false)
//
////  6. Complex Data with kay-value objects - seq
//  val map_properties_seq = (df: DataFrame) => {
//    val mappedDF = df
//      .withColumn("properties_map", map_from_arrays($"properties.key", $"properties.value"))
//      .drop("properties")
//
//    val keys = Seq("manufacturer", "model", "color", "hp", "year", "mileage")
//    val keyCols = keys.map(f=> col("properties_map").getItem(f).as(f))
//    mappedDF.select(col("*") +: keyCols:_*).drop("properties_map")
//  }
//
//  val carsDF_data_object_key_value_seq = spark
//    .read
//    .option("multiline","true")
//    .json("data/cars/data-object-key-value.json")
//
//  carsDF_data_object_key_value_collect.show(false)
//  carsDF_data_object_key_value_collect.flatten(Map("properties" -> map_properties_seq)).show(false)
//
////  7. Complex Data with kay-value objects - drop
//  val drop = (df: DataFrame) => { df.drop("properties") }
//
//  val carsDF_data_object_key_value_drop = spark
//    .read
//    .option("multiline","true")
//    .json("data/cars/data-object-key-value.json")
//
//  carsDF_data_object_key_value_drop.show(false)
//  carsDF_data_object_key_value_drop.flatten(Map("properties" -> drop)).show(false)

}
