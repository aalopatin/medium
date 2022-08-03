package com.github.aalopatin.implicits

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import scala.annotation.tailrec

object Helpers {
  implicit class DataFrameHelper(df: DataFrame) {
    def flatten(custom: Map[String, DataFrame => DataFrame] = Map.empty): DataFrame = {

      @tailrec
      def flatten(schemaIterator: Iterator[StructField], accumulator: DataFrame): DataFrame = {
        if (schemaIterator.hasNext) {
          val structField = schemaIterator.next()
          val column = structField.name
          val fieldType = if (custom.contains(column)) "custom" else structField.dataType.typeName
          val accum = fieldType match {
            case "struct" =>
              accumulator
                .changeColumn(column) {
                  ColumnUtils.flatColumn
                }
            case "array" | "map" =>
              accumulator
                .changeColumn(column) {
                  ColumnUtils.explodeColumn
                }
                .withColumnRenamed(s"${column}_exploded", column)
            case "custom" =>
              custom(column)(accumulator)
          }
          flatten(schemaIterator, accum)
        } else {
          val schemaIter = getSchemaIterator(accumulator)
          if (schemaIter.isEmpty) {
            accumulator
          } else {
            flatten(schemaIter, accumulator)
          }
        }
      }

      def getSchemaIterator(dataFrame: DataFrame) = {
        dataFrame.schema.iterator.filter(
          field =>
            Seq("struct", "array", "map") contains field.dataType.typeName
        )
      }

      val schemaIterator = getSchemaIterator(df)
      if (schemaIterator.nonEmpty)
        flatten(schemaIterator, df)
      else
        df

    }

    def changeColumn(column: String)(expression: String => Column): DataFrame = {
      df
        .select(col("*"), expression(column))
        .drop(s"$column")
    }

  }

  private object ColumnUtils {
     def flatColumn(column: String): Column = {
      col(s"$column.*")
    }
     def explodeColumn(column: String): Column = {
      explode(col(s"$column")).as(s"${column}_exploded")
    }
  }

}

