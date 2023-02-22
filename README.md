# Flight Data Analysis

Using the provided data from csv files, calculated some statistics from the data. The code output th result to the console

## Description

Questions: 

-Find the total number of flights for each month.
-Find the names of the 100 most frequent flyers.
-Find the greatest number of countries a passenger has been in without being in the UK. For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.
-Find the passengers who have been on more than 3 flights together.
-Find the passengers who have been on more than N flights together within the range (from,to).

## Getting Started


### Dependencies

- Java SDK installed and set jave environment 
- IntelliJ
- Scala plugin

-scalaVersion: "2.13.10"
-sbtVersion: "1.8.2"

-Libraries to install 
  "org.apache.spark" %% "spark-core" % 3.3.2"
  "org.apache.spark" %% "spark-sql" % 3.3.2"
  
-Resources
  flightData.csv
  passengers.csv

### Executing program

-Libraries to import
  import org.apache.spark.SparkConf
  import org.apache.spark.sql._
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._
  import java.sql.Date

-Run main.scala after the project is build

-For task:5
  The values of minFlights, fromDate, toDate can be change depending on the range required
      val minFlights = atleastNFlights
      val fromDate = from
      val toDate = date
      val toDate = date


