package advanced

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

/*Find movie wit the most ratings*/
object PopularMoviesNicerDataset {

  final case class Movie(MovieID :  Int)
  def main(args : Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("Movie").master("local[*]").getOrCreate()
    val movieSchema =  new StructType()
      .add("userID",IntegerType,nullable = true)
      .add("movieID",IntegerType,nullable = true)
      .add("rating",IntegerType,nullable = true)
      .add("timestamp",LongType,nullable = true)

    import spark.implicits.*
    val moviesDS = spark.read
      .option("sep", " ")
      .schema(movieSchema).csv("data/u.data")
      //.as[Movie]

    val topMoviesIDs = moviesDS.groupBy("movieID").count().orderBy(asc("movieID"))

    // Grab the top 10
    topMoviesIDs.show(10)

    // Stop the session
    spark.stop()
  }

}
