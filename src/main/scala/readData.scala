import Schema.{flightData, passengerData}

import scala.io.Source

case class readData() {

  /**
   * Gives the flight dataset
   * @param location location of the csv file
   * @return Seq[flightData]
   */
  def readFlightsData(location: String): Seq[flightData] = {
    val flight_data: Seq[flightData] = {
      for {
        line <- Source.fromFile(location).getLines().drop(1).toVector
        values = line.split(",").map(_.trim)
      } yield flightData(
        values(0).toInt
        , values(1).toInt
        , values(2)
        , values(3)
        , values(4)
      )
    }
    flight_data
  }

  /**
  Gives the passenger dataset
   * @param location location of the csv file
   * @return Seq[passengerData]
   * */
  def readPassengerData(location: String): Seq[passengerData] = {
    val passenger_data: Seq[passengerData] = {
      for {
        line <- Source.fromFile(location).getLines().drop(1).toVector
        values = line.split(",").map(_.trim)
      } yield passengerData(
        values(0).toInt
        , values(1)
        , values(2)
      )
    }
    passenger_data
  }
}
