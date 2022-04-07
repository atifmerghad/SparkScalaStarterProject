package basics.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._


object FriendsByAgeDataset {

  case class FakeFriends(id : Int, name: String, age: Int, friends: Int)

  /** Our main function where the actions happens */
  def main(args: Array[String]): Unit ={
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use SparkSession interface
    val spark = SparkSession.builder.appName("FriendsByAge").master("local[*]").getOrCreate()

    // Load each line of the source data into a Dataset
    /*
    val people = spark.read
      .option("header","true")
      .option("inferSSchema","true")
      .csv("data/fakefriends.csv").as[FakeFriends]
    */
    import spark.implicits._
    val ds = spark.read
      .format("csv")
      .option("delimiter",",")
      .schema("id  integer, name string, age integer, friends integer")
      .load("data/fakefriends.csv")

    // Select only line of the source data into an Dataset
    val friendsByAge = ds.select("age","friends")
    friendsByAge.show()
    // From friendsByAge we group by "age" and then compute average
    friendsByAge.groupBy("age").avg("friends").show()

    // Sorted:
    friendsByAge.groupBy("age").avg("friends").sort("age").show()

    // Formatted more nicely :
    friendsByAge.groupBy("age").agg( round( avg("friends")  ,2)).sort("age").show()

    // With a custom column name :
    friendsByAge.groupBy("age").agg( round( avg("friends")  ,2).alias("friends_avg")).sort("age").show()


    spark.stop()
  }

}
