package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingsCounter {
  def main( args: Array[String]): Unit ={
    println("Application is starting ...")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext(master = "local[*]", appName="RatingsCounter")
    val lines = sc.textFile("data/u.data")
    val ratings = lines.map( x => x.split("\t")(2) )
    val results = ratings.countByValue()
    val sortedResults = results.toSeq.sortBy(_._1)
  }
}
