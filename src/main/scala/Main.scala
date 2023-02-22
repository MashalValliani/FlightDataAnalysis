// Importing spark libraries
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.Date


object Main {
  def main(args: Array[String]) {

    // Setting up Spark Configuration
    val sparkConf = new SparkConf().setAppName("FlightDataAnalysis").setMaster("local").set("spark.cores.max", "4")

    // Create a Spark Session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // Setting  spark log to Error from info
    spark.sparkContext.setLogLevel("ERROR")

    // Reading flightData.csv and Passenger.csv files while ignoring the first header row
    val flightData = spark.read.option("header", true).csv("src/main/resources/flightData.csv")
    val passengerData = spark.read.option("header", true).csv("src/main/resources/passengers.csv")

    println("Total number of flights for each month")
    // Calling monthlyflights function while passing the flightData as parameter
    monthlyFlights(flightData)

    println("Names of the 100 most frequent flyers")
    // Calling frequentflyers function while passing the flightData and passengerData as parameters
    frequentflyers(flightData, passengerData)

    println("Greatest number of countries a passenger has been in without being in the UK")
    // Calling greatestCountriesWithoutUK function while passing the flightData as parameter
    greatestCountriesWithoutUK(flightData)

    println("Passengers who have been on more than 3 flights together")
    // Calling minThreeFlightsTogether function while passing the flightData as parameter
    minThreeFlightsTogether(flightData)

    println("Passengers who have been on more than N flights together within the range (from,to).")

    // Creating variables for minFlights and date range(from,to) to pass as parameters
    val minFlights = 10
    val fromDate = Date.valueOf("2017-01-07")
    val toDate = Date.valueOf("2017-07-31")

    // Calling flownTogether function while passing the flightData, minFlights, from and to as parameter
    flownTogether(flightData, minFlights, fromDate, toDate)

    // Closing Spark Session
    spark.close()
  }

  // Task:1 Find the total number of flights for each month

  // Defining function
  def monthlyFlights(flightData: DataFrame): Unit = {

    flightData.withColumn("month", month(col("date"))) // Dataframe (Fetching Month from Date Column)
      .groupBy(col("month"))                                    // Group by month
      .agg(count("flightId")                                 // Count FLights based on Month
        .as("Number_Of_Flights"))
      .orderBy(asc("month"))                                 // Order Data By Month in Ascending order
      .show()
  }

  // Task:2 Find the names of the 100 most frequent flyers
  // Defining function
  def frequentflyers(flightData: DataFrame, passengerData: DataFrame): Unit = {

    flightData.join(passengerData, flightData("passengerId") === passengerData("passengerId"), "inner")  // Join flightData and passengerData based on passengerID
      .groupBy(passengerData("passengerId"), passengerData("firstName"), passengerData("lastName"))               // Group by passengerID, firstName and LastName
      .agg(count("flightId").as("Number_Of_Flights"))                                           // Count Number of Flights based on passenger ID
      .orderBy(desc("Number_Of_Flights"))                                                             // Order Data By Number of Flights in Desc order
      .select("passengerId", "Number_Of_Flights", "firstName", "lastName")                             // Selecting Columns for Display
      .show(100)                                                                                        // Display first 100 rows
  }

  // Task:3 Find the greatest number of countries a passenger has been in without being in the UK
  def greatestCountriesWithoutUK(flightData: DataFrame): Unit = {

    // Declaring window for analytic functions
    val win = Window.partitionBy("passengerId").orderBy("date")

    // Defining new column to count no. of times passenger arrived UK
    val ukArrival = flightData.withColumn("newUK", sum(expr("case when from = 'uk' then 1 else 0 end")).over(win))

    // Calculating longest runs outside UK
    val runsCount = (
      ukArrival
        .groupBy("passengerId", "newUK")                            // Group Data by passengerId and newUK; for each UK-...-UK itinary
        .agg((
          sum(                                                                 // Aggregating over the results conditional expression
            expr(
              """
                  case
                      when 'uk' not in (from,to) then 1 -- count all nonUK countries, except for first one
                      when from = to then -1            -- special case for UK-UK itinaries
                      else 0                            -- don't count itinaries from/to UK
                  end""")
          )
          ).as("notUK"))
        .groupBy("passengerId")                                        // Group by Data based on passenger ID
        .agg(max("notUK").as("longestRun"))   // Count maximum of not UK column to determine longest run outside UK
      )

    runsCount.orderBy(desc("longestRun")).show()   // Sorting longestRun in descending order
  }

  // Task:4 Find the passengers who have been on more than 3 flights together
  // Defining function
  def minThreeFlightsTogether(flightData: DataFrame): Unit = {

    flightData
      .join(flightData.withColumnRenamed("passengerId", "passengerId2"), "flightId")    // Self Join flightData based on flightID
      .where(col("passengerId") < col("passengerId2"))                                                  // Filter out cases where the passengerId is equal to the passengerId2 (i.e., a passenger flying alone)
      .groupBy("passengerId", "passengerId2")                                                                 // Group by passengerID and passengerID2
      .agg(count("*").alias("FlightTogether"))                                                         // Count all based on passenger ID for each group
      .filter("FlightTogether > 3")                                                                          // Filter passenger with more than 3 flights together
      .orderBy(desc("FlightTogether"))                                                                       // Order Data By FlightTogether in Desc order
      .select("passengerId", "passengerId2", "FlightTogether")
      .show()
  }

  // Task:5 Find the passengers who have been on more than N flights together within the range (from,to)
  // Defining function
  def flownTogether(flightData: DataFrame, atleastNtimes: Int, from: Date, to: Date): Unit = {

    // Converting input dates to LocalDate
    val fromDate = from.toLocalDate
    val toDate = to.toLocalDate

    flightData
      .filter(col("date").between(fromDate.toString, toDate.toString))                                            // Filtering Dataframe to include only given dates
      .join(flightData.withColumnRenamed("passengerId", "passengerId2"), "flightId")     // Self Join flightData based on flightID
      .where(col("passengerId") < col("passengerId2"))                                                  // Filter out cases where the passengerId is equal to the passengerId2 (i.e., a passenger flying alone)
      .groupBy("passengerId", "passengerId2")                                                                 // Group by passengerID and passengerID2
      .agg(count("*").alias("FlightTogether"))                                                          // Count all based on passenger ID for each group
      .filter(s"FlightTogether >= ${atleastNtimes}")                                                         // Filter passenger with atleast N flights together
      .orderBy(desc("FlightTogether"))                                                                       // Order Data By FlightTogether in Desc order
      .withColumn("from", lit(fromDate))                                                                        // Adding from column with specified Starting date
      .withColumn("to", lit(toDate))                                                                           // Adding to column with specified Starting date
      .select("passengerId", "passengerId2", "FlightTogether", "from", "to")
      .show()
  }
}
