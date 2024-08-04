package com.flight.analytics.models

object Models {
  case class Flight(passengerId:Long, flightId: Long, from: String, to:String, date:String )
  case class Passenger(passengerId:Long, firstName: String, lastName: String )
  case class MonthlyFlightCount(month: Long, numberOfFlights: Long)
  case class FrequentFlyers(passengerId: Long, numberOfFlights: Long, firstName: String, lastName: String )
  case class PassengerWithMaxCountryTravelInBetween(passengerId: Long, longestRun: Long)
  case class PassengerPairsFlownTogether(passengerId1: Long, passengerId2: Long, flightCount: Long)
  case class PassengerPairsFlownTogetherInDateRange(passengerId1: Long, passengerId2: Long, from: String, to:String,
    flightCount: Long)
  case class PassengerLongestFlightHop(passengerId: Long, longestRun: Long)
}
