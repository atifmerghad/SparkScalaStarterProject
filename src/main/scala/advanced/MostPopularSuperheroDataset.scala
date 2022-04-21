package advanced

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, udf}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

import scala.io.{Codec, Source}
/*Find movie wit the most ratings*/
object PopularMoviesNicerDataset {

  final case class Movies(userID : Int, MovieID :  Int, rating : Int, timestamp : Long)

  /* Load up a Map of Movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {
  // Handle character encoding issues :
  implicit val codec : Codec = Codec("ISO-8859-1") // this the current encoding of u.item, not UTF-8.
  // Create a Map of Ints to String. and populate it from u.item
  var movieNames: Map[Int, String] = Map()
  val lines = Source.fromFile("data/movie/u.item")
  for( line <- lines.getLines()){
    val fields = line.split("|")
    if( fields.length > 1){
      movieNames += (fields(0).toInt -> fields(1))
    }
  }
    lines.close()
    movieNames
  }

  def main(args : Array[String]): Unit ={



    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("PopularMoviesNacer").master("local[*]").getOrCreate()

    val nameDist = spark.sparkContext.broadcast(loadMovieNames())

    // Create schema when reading u.data
    val movieSchema =  new StructType()
      .add("userID",IntegerType,nullable = true)
      .add("movieID",IntegerType,nullable = true)
      .add("rating",IntegerType,nullable = true)
      .add("timestamp",LongType,nullable = true)

    // Load up movie data as dataset
    import spark.implicits.*
    val movies = spark.read
      .option("sep", "\t")
      .schema(movieSchema).csv("data/movie/u.data")
      //.as[Movies]

    val movieCounts = movies.groupBy("movieID").count()

    // Create a UDF to look up movie names from our shared Map variable.
    // We start by declaring an anonymous function in scala
    val lookupName : Int => String = (movieID: Int) =>{
      nameDist.value(movieID)
    }
    // Then wrap it with a UDF
    val lookupNameUDF = udf(lookupName)

    // Add a movieTitle column using our new UDF
    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))

   // Sort the results
   val sortedMoviesWithNames  = moviesWithNames.sort("count")

    // Shows the results without truncating it
    sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate = false)


    // Stop the session
    spark.stop()
  }

}
