package com.flight.analytics.utils

import com.flight.analytics.models.Models.{Flight, Passenger}
import org.apache.spark.sql.{Dataset, SparkSession}

object CsvReader {
  /**
   * Reading into dataset from a CSV file in the resources folder.
   *
   * @param spark Implicit SparkSession.
   * @param fileName The name of the file to read the CSV.
   * @return
   */
  def readFlightCsv(spark:SparkSession, fileName: String): Dataset[Flight] = {
    import spark.implicits._

    // Use the fully qualified path for getClass to avoid ambiguity
    val filePath = this.getClass.getResource(fileName).getPath

    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(filePath)
      .as[Flight]
  }

  def readPassengerCsv(spark:SparkSession, fileName: String): Dataset[Passenger] = {
    import spark.implicits._

    // Use the fully qualified path for getClass to avoid ambiguity
    val filePath = this.getClass.getResource(fileName).getPath

    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(filePath)
      .as[Passenger]
  }
}
