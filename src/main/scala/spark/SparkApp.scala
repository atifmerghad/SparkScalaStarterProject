package spark

import org.apache.spark._
import org.apache.log4j._

object SparkApp extends App{

  println("Application is starting ...")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext(master = "local[*]", appName="RatingsCounter")

  val lines = sc.textFile("data/u.data")

  val ratings = lines.map( x => x.split("\t")(2) )

  ratings.foreach(println)

  val results = ratings.countByValue()

  val sortedResults = results.toSeq.sortBy(_._1)

}
