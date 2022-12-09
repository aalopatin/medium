package com.github.aalopatin

object Columns {
  val Product = "product"
  val TimeBucket = "timebucket"
  val ValidFrom = "validfrom"
  val Rate = "rate"

  val CompositeKey = Seq(Product, TimeBucket)
  val ColumnsAll = Seq(Product, TimeBucket, ValidFrom, Rate)
}
