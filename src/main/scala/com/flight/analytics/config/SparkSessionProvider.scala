package com.flight.analytics.config
import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }
}
