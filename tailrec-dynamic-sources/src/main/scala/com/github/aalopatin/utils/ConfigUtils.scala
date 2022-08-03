package com.github.aalopatin
package utils

import com.github.aalopatin.implicits.Helpers.ConfigHelper
import com.typesafe.config.ConfigFactory

object ConfigUtils {

  val conf = ConfigFactory.load()

  def getSources = {
    conf.getMap("sources")
  }

  def getJDBC = {
    conf.getMap("jdbc")
  }
}
