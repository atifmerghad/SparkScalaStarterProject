package basics.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.lower


object WordCountBetterSortedDataset {
  case class Book( value : String )
  case class Test1(name: String, age: Int, place: String)

  def main(args : Array[String] ): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("WordCountBetterSortedDataset").master("local[*]").getOrCreate()

    import spark.implicits._
    val input = spark.read.text("data/book.txt")//.as[Book]
    println("type : "+input.dtypes)
    input.show()

    //Split using a regular expression that extracts words
    //val words = input.select(explore(split($"value", "\\W+")).alias("word")).filter($"word" != "")

    // Normalize everything to lowercase
   // val lowercaseWords = words.select( lower($"word").alies("word"))

   // Count up the occurrences of each word
   //val wordCounts = lowercaseWords.groupBy("word").count()

   //Sort by count
   //val wordCountSorted = wordCounts.sort("count")

   // ANOTHER WAY TO DO IT : ( BLENDING RDD's and Datasets )
   val bookRDD = spark.sparkContext.textFile("data/book.txt")
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = wordsRDD.toDS()

    val lowercaseWordDS = wordsDS.select(lower($"value").alias("word"))
    val wordCountsDS = lowercaseWordDS.groupBy("word").count()
    val wordCountsSortedDS = wordCountsDS.sort("count")
    wordCountsSortedDS.show(wordCountsSortedDS.count.toInt)
  }

}

