package basics.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = SparkContext("local[*]", "WordCount")

    val input = sc.textFile("data/book.txt")

    //Split into words separated by a space character
    val words = input.flatMap(line => line.split(" "))

    // Count up the occurrences for each word
    val wordCounts = words.countByValue()

    //Print the result
    wordCounts.foreach(println)
  }

}
