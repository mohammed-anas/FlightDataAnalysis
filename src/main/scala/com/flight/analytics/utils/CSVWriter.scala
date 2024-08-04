package com.flight.analytics.utils

import org.apache.spark.sql.{Dataset, SparkSession}
import java.io.{File, FileWriter, BufferedWriter}

object CSVWriter {

  /**
   * Writes a Dataset to a CSV file in the resources folder.
   *
   * @param dataset The Dataset to be written to the CSV file.
   * @param fileName The name of the file to write the CSV data to.
   * @param spark Implicit SparkSession.
   * @tparam T The type of the Dataset.
   */
  def writeToCSV[T](dataset: Dataset[T], fileName: String)(implicit spark: SparkSession): Unit = {

    // Resolve the file path in the resources directory
    val resourcePath = Option(this.getClass.getResource(fileName))
      .map(_.getPath)
      .getOrElse {
        // If resource path is not available, use the project's resource folder
        val filePath = s"src/main/resources/$fileName"
        // Ensure the directory exists
        val file = new File(filePath)
        if (!file.getParentFile.exists()) {
          file.getParentFile.mkdirs()
        }
        filePath
      }

    println(s"Writing data to $resourcePath")

    // Write the Dataset to CSV
    dataset.write
      .mode("overwrite")
      .option("header", "true")
      .csv(resourcePath)

    println(s"Results have been written to $resourcePath")
  }
}
