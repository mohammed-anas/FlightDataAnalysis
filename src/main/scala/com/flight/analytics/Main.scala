package com.flight.analytics

import com.flight.analytics.config.SparkSessionProvider
import com.flight.analytics.service.DataProcessor
import com.flight.analytics.utils.{CSVWriter, CsvReader}


object Main {
  def main(args: Array[String]): Unit = {

    // Initialize SparkSession
    val spark = SparkSessionProvider.getSparkSession("Flight Data Analytics")

    // Read flight data
    val flightFileName = "/input/flights.csv"
    val flightDs = CsvReader.readFlightCsv(spark, flightFileName)

    // Read passenger data
    val passengerFileName = "/input/passengers.csv"
    val passengerDs = CsvReader.readPassengerCsv(spark, passengerFileName)


    CSVWriter.writeToCSV(
      DataProcessor.getFlightCountEachMonth(flightDs)(spark),
      "output/FlightCountEachMonth"
    )(spark)


    CSVWriter.writeToCSV(
      DataProcessor.getFrequentFlyers(flightDs, passengerDs, 100)(spark),
      "output/FrequentFlyers"
    )(spark)

    CSVWriter.writeToCSV(
      DataProcessor.flownTogetherMoreThanNFlights(flightDs, 3)(spark),
      "output/FlownTogetherMoreThanNFlights"
    )(spark)

    DataProcessor.longestFlightHopWithoutUK(flightDs)(spark).show()
    CSVWriter.writeToCSV(
      DataProcessor.longestFlightHopWithoutUK(flightDs)(spark),
      "output/LongestRunByPassenger.csv"
    )(spark)

  }
}
