import Schema.{passengerData, flightData}

import scala.io.Source

object Main {

  def main(args: Array[String]): Unit = {

    val flight_file = s"${os.pwd}\\src\\main\\scala\\Data\\f_Data.csv"
    val passenger_file = s"${os.pwd}\\src\\main\\scala\\Data\\p_Data.csv"

    val flight_data:Seq[flightData] = {
      for {
        line <- Source.fromFile(flight_file).getLines().drop(1).toVector
        values = line.split(",").map(_.trim)
      } yield flightData(
        values(0).toInt
        , values(1).toInt
        , values(2)
        , values(3)
        , values(4)
      )
    }

    val passenger_data:Seq[passengerData] = {
      for {
        line <- Source.fromFile(passenger_file).getLines().drop(1).toVector
        values = line.split(",").map(_.trim)
      } yield passengerData(
        values(0).toInt
        , values(1)
        , values(2)
      )
    }

    solutionSparkSQL().flightsPerMonth(flight_data)

    solutionSparkSQL().frequentFlyers(flight_data, passenger_data)

    solutionSparkSQL().countriesVisited(flight_data)

    solutionSparkSQL().passengerTogether(flight_data)

    solutionSparkSQL().passengerTogetherRange(flight_data, "2017-01-01", "2017-10-31", 5)
  }
}