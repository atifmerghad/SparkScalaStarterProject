package advanced

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.jdk.LongAccumulator

object DegreesOfSeparation {

  // The characters we want to find the separation between.
  val startCharacterID = 5306 // Spiderman
  val targetCharacterID = 14  // ADAM 3,031 (who?)

  // We make our accumulator a "global" Option so we can reference it in mapper later.
  var hitCounter: Option[LongAccumulator] = None

  // Some custom data types
  // BFSData contains an array of hero ID connections, the distance, and color.
  type BFSData = (Array[Int], Int, String)
  // A BFSNode has a heroID and the BFSData associated with it.
  type BFSNode = (Int, BFSData)

  /** Convert a line of raw input into a BFSNode **/
  def convertToBFS(line : String) : BFSNode ={
    val fields = line.split("\\s+")

    // Extract the heroID from the first field
    val heroID = fields(0).toInt
    // Extract Subsequent hero ID's into the connections array
    var connections: ArrayBuffer[Int] = ArrayBuffer()

    for(connection <- 1 until( fields.length -1)){
      connections+= fields(connection).toInt
    }

    // Default distance and color is 9999 and white
    var color: String = "WHITE"
    var distance: Int = 9999

    // Unless this is the character we're starting from
    if(heroID == startCharacterID ){
      color = "GRAY"
      distance = 0
    }

    (heroID, (connections.toArray, distance, color))
  }

  /** Create "iteration 0" of our RDD of BFSNodes **/
  def createStartingRdd(sc: SparkContext) : RDD[BFSNode] = {
    val inputFile = sc.textFile("data/movie/Marvel-Graph.txt")
    inputFile.map(convertToBFS)
  }

  /*
  def BFSMap(node : BFSNode): Array[BFSNode] = {

  }
  */
}
