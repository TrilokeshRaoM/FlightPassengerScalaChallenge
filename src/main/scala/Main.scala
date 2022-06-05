import Schema.{flightData, passengerData}

object Main {

  def main(args: Array[String]): Unit = {

    val flight_file = s"${os.pwd}\\src\\main\\scala\\Data\\f_Data.csv"
    val passenger_file = s"${os.pwd}\\src\\main\\scala\\Data\\p_Data.csv"

    val flight_data: Seq[flightData] = readData().readFlightsData(flight_file)
    val passenger_data: Seq[passengerData] = readData().readPassengerData(passenger_file)

    solutionSparkSQL().flightsPerMonth(flight_data)

    solutionSparkSQL().frequentFlyers(flight_data, passenger_data)

    solutionSparkSQL().countriesVisited(flight_data)

    solutionSparkSQL().passengerTogether(flight_data)

    solutionSparkSQL().passengerTogetherRange(flight_data, "2017-01-01", "2017-10-31", 5)

  }
}