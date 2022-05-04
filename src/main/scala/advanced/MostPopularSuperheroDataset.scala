package advanced

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Encoders}
import org.apache.spark.sql.functions.{sum,asc, col, split, udf, size}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

import scala.io.{Codec, Source}

/*Find movie wit the most ratings*/
object MostPopularSuperheroDataset {

  case class SuperHeroNames(id : Int, name: String)
  case class SuperHero(value : String)

  def main(args : Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("MostPopularSuperhero").master("local[*]").getOrCreate()

    // Create schema when reading
    val superHeroNamesSchema = new StructType()
      .add("id",IntegerType,nullable = true)
      .add("name",StringType,nullable = true)

    // Build up a hero ID -> name Dataset
    import spark.implicits._
    val names = spark.read.option("sep", " ").schema(superHeroNamesSchema).csv("data/movie/Marvel-names.txt") //.as[SuperHeroNames]

    val lines = spark.read.text("data/movie/Marvel-graph.txt").as[String] //.as[SuperHero]
    lines.printSchema()

    val connections = lines
          .withColumn("id", split(col("value"), " ")(0))
          .withColumn("connections", size(split(col("value"), " ")) -1)
          .groupBy("id").agg(sum("connections").alias("connections"))

    connections.show()

    val mostPopular =  connections.sort($"connections".desc).first()

    println("mostPopular : "+ mostPopular)


    val mostPopularName = names
          .filter($"id" === mostPopular(0))
          .select("name")
          .first()

        println(s"${mostPopularName(0)} is the most popular superhero with ${mostPopular(1)} co-appearances.")

        // Stop the session
        spark.stop()

  }

}
