package com.flight.analytics.service

import com.flight.analytics.models.Models.{Flight, FrequentFlyers, MonthlyFlightCount, Passenger, PassengerLongestFlightHop, PassengerPairsFlownTogether, PassengerPairsFlownTogetherInDateRange}
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter


object DataProcessor {
  /**
   * Count number of flights each month
   *
   * @param flightDS Content of flight csv into a dataset
   * @param spark Implicit SparkSession.
   * @return dataset which contains monthly flight count
   * @see [[MonthlyFlightCount]]
   */
  def getFlightCountEachMonth(flightDS: Dataset[Flight])(implicit spark: SparkSession): Dataset[MonthlyFlightCount] = {
    import spark.implicits._

    def cleanDateString(dateStr: String): String = {
      dateStr.split(" ")(0)
    }
    def parseDate(dateStr: String): LocalDate = {
      val cleanedDateStr = cleanDateString(dateStr)
      val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      LocalDate.parse(cleanedDateStr, dateFormatter)
    }

    // Convert the date string to LocalDate and extract year and month
    val flightsWithYearMonth = flightDS.map { flight =>
      val date = parseDate(flight.date)  // Use the method here
      val month = date.getMonthValue
      (month)
    }

    val flightCounts = flightsWithYearMonth
      .groupByKey(identity)
      .count()

    // Convert to MonthlyFlightCount
    val monthlyFlightCounts = flightCounts.map {
      case (month, count) =>
        MonthlyFlightCount(month, count)
    }

    monthlyFlightCounts
  }

  /**
   * Reading a flight dataset and return passenger's total flight count
   *
   * @param flightDS Content of flight csv into a dataset
   * @param spark Implicit SparkSession.
   * @return intermediate dataset which contains count of flight per passenger Id
   */
  def totalFlightTakenByPassenger(flightDS: Dataset[Flight])
                                 (implicit spark: SparkSession):Dataset[(Long, Long)] = {
    import spark.implicits._

    val flightCountDS: Dataset[(Long, Long)] = flightDS.groupByKey(_.passengerId).mapGroups{ case (passengerId,
    flights) => (passengerId, flights.size.toLong)}
    flightCountDS
  }

  /**
   * Perform join with passenger csv data on passenger Id to obtain passenger information
   *
   * @param passengerFlightCount intermediate dataset which contains count of flight per passenger Id
   * @param passengers Content of passenger csv into a dataset
   * @param spark Implicit SparkSession.
   * @return Dataset of FrequentFlyer.
   * @see [[FrequentFlyers]]
   */
  def performJoinOnPassengerId(passengerFlightCount: Dataset[(Long, Long)], passengers: Dataset[Passenger])
                              (implicit spark: SparkSession): Dataset[FrequentFlyers] = {

    import spark.implicits._

    val passengerFlightCountsDS: Dataset[FrequentFlyers] = passengerFlightCount
      .joinWith(passengers, passengerFlightCount("_1") === passengers("passengerId"))
      .map { case ((passengerId, flightCount), passenger) =>
        FrequentFlyers(passengerId, flightCount, passenger.firstName, passenger.lastName)
      }
    passengerFlightCountsDS

  }

  /**
   * Get n most frequent flyers
   *
   * @param flightDS Content of flight csv into a dataset
   * @param passengerDS Content of passenger csv into a dataset
   * @param limit maximum number of results.
   * @param spark Implicit SparkSession.
   * @return Dataset of FrequentFlyer.
   * @see [[FrequentFlyers]]
   */
  def getFrequentFlyers(flightDS: Dataset[Flight], passengerDS: Dataset[Passenger], limit: Int )(implicit
                                                                                            spark: SparkSession)
  : Dataset[FrequentFlyers] = {
    import spark.implicits._

    val flightCountDS: Dataset[(Long, Long)] = totalFlightTakenByPassenger(flightDS)
    val passengerFlightCountsDS: Dataset[FrequentFlyers] = performJoinOnPassengerId(flightCountDS, passengerDS)

    passengerFlightCountsDS.orderBy($"numberOfFlights".desc)
      .limit(limit)
  }

  /**
   * Generates list of passenger pair tuples.
   *
   * @param flightDS Content of flight csv into a dataset
   * @param spark Implicit SparkSession.
   * @return dataset of tuples
   */
  def getPassengerPairs(flightDS: Dataset[Flight])(implicit spark: SparkSession): Dataset[(Long, Long)] = {
    import spark.implicits._

    // Generate pairs of passengers who were on the same flight
    val passengerPairsDS: Dataset[(Long, Long)] = flightDS
      .groupByKey(_.flightId)
      .flatMapGroups { case (_, flights) =>
        val passengerIds = flights.map(_.passengerId).toSeq
        val pairs = for {
          p1 <- passengerIds
          p2 <- passengerIds if p1 < p2
        } yield (p1, p2)
        pairs
      }
    passengerPairsDS
  }

  /**
   * Count the passenger pair tuple count by grouping them.
   *
   * @param passengerPairs passenger pair tuples list
   * @param spark Implicit SparkSession.
   * @return dataset return passenger pairs tuple with its count
   */
  def getPassengerPairCount(passengerPairs: Dataset[(Long,Long)])(implicit spark: SparkSession):Dataset[(Long, Long, Long)] = {
    import spark.implicits._
    passengerPairs.groupByKey(identity).mapGroups{
      case ((p1, p2), iter) => (p1, p2, iter.size.toLong)
    }
  }

  /**
   * Calculate passengers pair who flew together in same flight more than n times.
   *
   * @param flightDS Content of flight csv into a dataset
   * @param flightCount threshold count for maximum flight trip together
   * @param spark Implicit SparkSession.
   * @return dataset of PassengerPairsFlownTogether
   * @see [[PassengerPairsFlownTogether]]
   */
  def flownTogetherMoreThanNFlights(flightDS: Dataset[Flight], flightCount : Long)(implicit spark: SparkSession)
  : Dataset[PassengerPairsFlownTogether] = {
    import spark.implicits._

    val passengerPairs: Dataset[(Long,Long)] = getPassengerPairs(flightDS)
    val passengerPairCount: Dataset[(Long, Long, Long)] = getPassengerPairCount(passengerPairs)

    passengerPairCount.filter(_._3 >= flightCount).orderBy($"_3".desc).map{
      case (p1,p2,count) => PassengerPairsFlownTogether(p1,p2,count)
    }
  }

  /**
   * filtering the flight data based upon date range
   *
   * @param flightDS Content of flight csv into a dataset
   * @param fromDate start date
   * @param toDate   end date
   * @param spark Implicit SparkSession.
   * @return dataset of flight
   * @see [[Flight]]
   */
  def getDateRangeFlights(flightDS: Dataset[Flight], fromDate: String, toDate: String) (implicit spark: SparkSession)
  : Dataset[Flight] = {

    flightDS
      .filter(flight => flight.date >= fromDate && flight.date <= toDate)
      .orderBy(asc("date"))
  }

  /**
   *
   * @param flightDS
   * @param flightCount
   * @param fromDate
   * @param toDate
   * @param spark
   * @return
   */
def flownTogetherMoreThanNFlights(flightDS: Dataset[Flight], flightCount: Long, fromDate: String, toDate: String)
                                 (implicit spark: SparkSession)
: Dataset[PassengerPairsFlownTogetherInDateRange] = {

  import spark.implicits._


  // Filter flights by date range
  val dateRangeFlightsDS: Dataset[Flight] = getDateRangeFlights(flightDS, fromDate, toDate)

  // Generate passenger pairs for each flight
  val passengerPair: Dataset[(Long, Long, String)] = dateRangeFlightsDS
    .groupByKey(_.flightId)
    .flatMapGroups { case (_, flights) =>
      val flightList = flights.toList
      val passengerIds = flightList.map(_.passengerId).distinct
      for {
        i <- passengerIds.indices
        j <- i + 1 until passengerIds.length
      } yield (passengerIds(i), passengerIds(j), flightList.head.date)
    }

  // Aggregate pairs to count occurrences and find earliest/latest dates
  val aggregatedPairs: Dataset[(Long, Long, Long, String, String)] = passengerPair
    .groupByKey { case (p1, p2, _) => (p1, p2) }
    .mapGroups { case ((p1, p2), iter) =>
      val list = iter.toList
      val count = list.length.toLong
      val fromDate = list.map(_._3).min
      val toDate = list.map(_._3).max
      (p1, p2, count, fromDate, toDate)
    }

  // Filter and map the result
  aggregatedPairs
    .filter(_._3 >= flightCount)
    .orderBy($"_3".desc)
    .map { case (p1, p2, count, fromDate, toDate) =>
      PassengerPairsFlownTogetherInDateRange(p1, p2, count, fromDate, toDate)
    }
}

  /**
   * Group all flights for a given passenger sorted by asc order
   *
   * @param flightDS Content of flight csv into a dataset
   * @param spark Implicit SparkSession.
   * @return dataset of passengerId with its flights sequence
   */
  def passengerGroupSortByDate(flightDS: Dataset[Flight])(implicit spark: SparkSession): Dataset[(Long,Seq[Flight])] = {
    import spark.implicits._
    flightDS.groupByKey(_.passengerId)
      .mapGroups{ case (passengerId,flights) => (passengerId, flights.toSeq.sortBy(_.date))}
  }

  /**
   * Calculate longest run
   *
   * @param flightList sequence of flights
   * @param spark Implicit SparkSession.
   * @return length of longest run
   */

  def calculateLongestRun(flightList: Seq[Flight])(implicit spark: SparkSession): Long = {
    var maxHopLength = 0;
    var currentHopLength = 0;
    flightList.foreach{ flight =>
      if(flight.from != "uk" && flight.to != "uk"){
        currentHopLength += 1
        maxHopLength = Math.max(maxHopLength, currentHopLength)
      }else {
        currentHopLength = 0
      }
    }
    maxHopLength
  }

  /**
   *  Calculate the longest run per passenger not in UK in desc order of the length
   *
   * @param flightDS Content of flight csv into a dataset
   * @param spark Implicit SparkSession.
   * @return dataset of PassengerLongestFlightHop
   * @see [[PassengerLongestFlightHop]]
   */
  def longestFlightHopWithoutUK(flightDS: Dataset[Flight])(implicit spark: SparkSession)
  :Dataset[PassengerLongestFlightHop] = {

    import spark.implicits._
    val passengerGroups:Dataset[(Long,Seq[Flight])] = passengerGroupSortByDate(flightDS)
    passengerGroups.map{ case(passengerId, flightList) =>
      val longestRun: Long = calculateLongestRun(flightList)
      PassengerLongestFlightHop(passengerId, longestRun)
    }.orderBy($"longestRun".desc)
  }
}
