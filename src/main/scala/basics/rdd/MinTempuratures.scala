package basics.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math.min

object MinTempuratures {
  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val tempurature = fields(3).toFloat * 0.1f * (90f / 5.0f) + 32.0f
    (stationID, entryType, tempurature)
  }

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile("data/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val rdd = lines.map(parseLine)

    // Filter out all but TMIN entries
    val filteredData = rdd.filter(x => x._2 == "TMIN")

    // Convert to (stationID, temperature)
    val stationTemps = filteredData.map(x => (x._1, x._3.toFloat))

    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y))

    // Collect, format, and print the results
    val results = minTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }
  }
}
