package basics.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCountBetter {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = SparkContext("local[*]", "WordCount")

    // Load each line of my book into an RDD
    val input = sc.textFile("data/book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = words.countByValue()

    //Print the result
    wordCounts.foreach(println)
  }

}
