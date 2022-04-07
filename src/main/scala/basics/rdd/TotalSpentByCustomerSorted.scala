package basics.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalSpentByCustomerSorted {

  /*Convert input data to (customerId, amountSpent) tuples */
  def extractCustomerPricesPairs(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  /** Our main functions where the actions happens */
  def main(args: Array[String]): Unit = {
    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Create a SparkContext using every core of the local machine
    val sc = SparkContext("local[*]", "TotalSpentByCustomer")
    val lines = sc.textFile("data/customer-orders.csv")
    val mappedInput = lines.map(extractCustomerPricesPairs)

    val totalByCustomer = mappedInput.reduceByKey((x, y) => x + y)

    val flipped = totalByCustomer.map(x => (x._2, x._1))

    val totalByCustomerSorted = flipped.sortByKey()

    val results = totalByCustomerSorted.collect()

    // Print the result
    results.foreach(println)
  }
}
