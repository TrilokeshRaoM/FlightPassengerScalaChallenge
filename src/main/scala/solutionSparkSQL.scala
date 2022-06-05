import Schema.{flightData, passengerData}
import org.apache.spark.sql.SparkSession

case class solutionSparkSQL(){

  val appName = "Scala Example - List to Spark Data Frame"
  val master = "local"
  val spark = SparkSession
    .builder
    .appName(appName)
    .master(master)
    .getOrCreate()

  import spark.implicits._


  /**
   * This method is used to get the total number of flights that have travelled per month.
   * Currently the dataset has only one years data 2017, hence we do not have a column for year.
   * @param flight_ds This is flight data of type Seq[flightData]
   */
  def flightsPerMonth(flight_ds: Seq[flightData]): Unit = {

    val flight_rdd = spark.sparkContext.parallelize(flight_ds)
    val flight_df = flight_rdd.toDF

    flight_df.createOrReplaceTempView("flight_view")

    val sql_df = spark.sql(
      s"""
         |SELECT
         |	month(to_date(date, 'yyyy-MM-dd')) Month
         |	, COUNT(*) Number_Of_Flights
         |FROM flight_view
         |GROUP BY 1
         |ORDER BY 1
         |""".stripMargin)


    sql_df.show(100, false)
  }


  /**
   * This function would give us the passenger details of the top 100 frequent flyers
   * @param flight_ds This is flight data of type Seq[flightData]
   * @param passenger_ds This is passenger data of type Seq[passengerData]
   */
  def frequentFlyers(flight_ds: Seq[flightData], passenger_ds: Seq[passengerData]): Unit = {

    val flight_rdd = spark.sparkContext.parallelize(flight_ds)
    val passenger_rdd = spark.sparkContext.parallelize(passenger_ds)

    val flight_df = flight_rdd.toDF
    val passenger_df = passenger_rdd.toDF

    flight_df.createOrReplaceTempView("flight_view")
    passenger_df.createOrReplaceTempView("passenger_view")

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "10")

    val sql_df = spark.sql(
      s"""
         |WITH top_100_passengers as (
         |  SELECT
         |	  passengerid passenger_id
         |	  , count(*) number_of_flights
         |  FROM flight_view
         |  GROUP BY 1
         |  ORDER BY count(*) DESC
         |  LIMIT 100
         |)
         |SELECT
         |	t.*
         |	, p.firstname first_name
         |	, p.lastname last_name
         |from top_100_passengers t
         |	LEFT OUTER JOIN passenger_view p
         |		ON t.passenger_id = p.passengerid
         |ORDER BY t.number_of_flights desc
         |""".stripMargin)

    sql_df.show(1000, false)
  }


  /**
   * This gives us the longest run of each passenger considering the departing country is not UK
   * @param flight_ds This is flight data of type Seq[flightData]
   */
  def countriesVisited(flight_ds: Seq[flightData]): Unit = {

    val flight_rdd = spark.sparkContext.parallelize(flight_ds)
    val flight_df = flight_rdd.toDF

    flight_df.createOrReplaceTempView("flight_view")

    val sql_df = spark.sql(
      s"""
         |SELECT passengerid passenger_id
         |, count(*) longest_run
         |FROM flight_view
         |WHERE LOWER(from) <> 'uk'
         |GROUP BY 1
         |ORDER BY 2 DESC
         |""".stripMargin)

    sql_df.show(100, false)
  }


  /**
   * This function gives the combination of passengers that have had atleast 3 flights taken in common
   * @param flight_ds This is flight data of type Seq[flightData]
   */
  def passengerTogether(flight_ds: Seq[flightData]): Unit = {

    val flight_rdd = spark.sparkContext.parallelize(flight_ds)
    val flight_df = flight_rdd.toDF

    flight_df.createOrReplaceTempView("flight_view")

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "10")

    val sql_df = spark.sql(
      s"""
         |SELECT
         |	passenger_1_id
         |	, passenger_2_id
         |	,count(*) number_of_flights_together
         |from (
         |	SELECT f1.passengerid as passenger_1_id
         |		, f2.passengerid as passenger_2_id
         |	FROM flight_view f1
         |		INNER JOIN flight_view f2
         |			ON f1.flightId = f2.flightId
         |			AND f1.`date` = f2.`date`
         |			AND f1.passengerid < f2.passengerid
         |)
         |GROUP BY 1,2
         |HAVING count(*) > 3
         |order by 3
         |""".stripMargin)

    sql_df.show(100, false)
  }


  /**
   * This function gives the combination of passengers that have had atleast n number of flights taken in common in a
   * given time period. the details of which are by the variables defined below.
   * @param flight_ds This is flight data of type Seq[flightData]
   * @param from_date The from date repersented as a String in format 'yyyy-MM-dd'
   * @param to_date The to date repersented as a String in format 'yyyy-MM-dd'
   * @param atleast_times minimum number of times the passengers have travelled together of type Int
   */
  def passengerTogetherRange(flight_ds: Seq[flightData], from_date: String, to_date: String, atleast_times: Int): Unit = {

    val flight_rdd = spark.sparkContext.parallelize(flight_ds)
    val flight_df = flight_rdd.toDF

    flight_df.createOrReplaceTempView("flight_view")

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "10")

    val sql_df = spark.sql(
      s"""
         |SELECT
         |	passenger_1_id
         |	, passenger_2_id
         |	,count(*) number_of_flights_together
         |from (
         |	SELECT f1.passengerid as passenger_1_id
         |		, f2.passengerid as passenger_2_id
         |	FROM flight_view f1
         |		INNER JOIN flight_view f2
         |			ON f1.flightId = f2.flightId
         |			AND f1.`date` = f2.`date`
         |			AND f1.passengerid < f2.passengerid
         |      AND to_date(f1.`date`, 'yyyy-MM-dd') >= to_date('$from_date', 'yyyy-MM-dd')
         |      AND to_date(f1.`date`, 'yyyy-MM-dd') <= to_date('$to_date', 'yyyy-MM-dd')
         |)
         |GROUP BY 1,2
         |HAVING count(*) > ${atleast_times -1}
         |order by 3
         |""".stripMargin)

    sql_df.show(100, false)
  }

}
