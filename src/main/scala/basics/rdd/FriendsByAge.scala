package basics.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge {

  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    // Tuple
    return (age, numFriends)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "FriendsByAge")
    val lines = sc.textFile("data/fakefriends-noheader.csv")

    val rdd = lines.map(parseLine)

    //  rdd.mapValues( x => (x,1)) : (33,385) => (33, (385, 1))
    // reduceByKey((x,y) => ( x._1 + y._1 , x._2 + y._2)) : (33,(385,2))
    val totalsbyAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val averagesByAge = totalsbyAge.mapValues(x => x._1 / x._2)

    // Collect the results from the RDD ( this kicks off computing the DAG and actually executes the job )
    val results = averagesByAge.collect()

    // Sort and print the final result
    results.sorted.foreach(println)

  }

}
