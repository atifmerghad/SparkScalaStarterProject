package advanced

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, sum, min}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MostObscureSuperheroes {
  case class SuperHeroNames(id : Int, name: String)
  case class SuperHero(value : String)

  def main(args : Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("MostObscureSuperheroes").master("local[*]").getOrCreate()

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

    val minConnectionCount =  connections.agg(min($"connections")).first().getLong(0)

    val minConnections = connections.filter($"connections" === minConnectionCount)

    val minConnectionsWithNames = minConnections.join(names, usingColumn = "id")

    println("The following characters have only "+ minConnectionCount + " connection(s):")

    minConnectionsWithNames.select("name").show()
    // Stop the session
    spark.stop()

  }
}
