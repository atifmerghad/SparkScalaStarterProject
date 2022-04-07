package basics.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}


object DataFramesDataset {

  case class Person(id : Int, name: String, age: Int, friends: Int)

  /** Our main function where the actions happens */
  def main(args: Array[String]): Unit ={
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use SparkSession interface
    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()

    // Load each line of the source data into a Dataset

    val people = spark.read
      .format("csv")
      .option("delimiter",",")
      .schema("id  integer, name string, age integer, friends integer")
      .load("data/fakefriends.csv")

    println("here is our inferred schema :")
    people.printSchema()

    println("Let's select the name column: ")
    people.select("name").show()

    println("Filter out anyone over 21: ")
    people.filter( people("age") < 21 ).show()

    println("Group by age : ")
    people.groupBy("age").count().show()

    println("Make everyone 10 years older : ")
    people.select(people("name"), people("age")+10).show()

    spark.stop()
  }

}
