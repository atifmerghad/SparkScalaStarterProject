package courses

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object HelloWorld {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("Starting ...")

   // val sc = SparkContext("local[*]", "HelloWorld")
   val sc = SparkSession.builder.appName("HelloWorld").master("local[*]").getOrCreate()

    println("sc version :"+sc.version)

    val lines = sc.sparkContext.textFile("data/u.data")

    val numLines = lines.count()

    println("Hello world! the u.data file has "+ numLines + " lines.")

    sc.stop()
  }

}
