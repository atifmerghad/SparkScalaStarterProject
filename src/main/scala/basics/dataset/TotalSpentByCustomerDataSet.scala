package basics.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StringType}
import org.apache.spark.sql.functions.{sum, round}
object TotalSpentByCustomerDataSet {

  case class CustomerOrders(cust_id :  Int,  irem_id: Int, amount_spent: Double)

  /** Our main functions where the actions happens */
  def main(args: Array[String]): Unit = {
    //Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create a SparkContext using every core of the local machine
    val spark = SparkSession.builder.appName("TotalSpentByCustomer Dataset").master("local[*]").getOrCreate()

    val customerOrdersSchema =  new StructType()
      .add("cust_id", StringType, nullable=true)
      .add("irem_id", StringType, nullable=true)
      .add("amount_spent", StringType, nullable=true)


    val customerDS  = spark.read.schema(customerOrdersSchema).csv("data/customer-orders.csv")//.as[CustomerOrders]

    val totalByCustomer = customerDS
      .groupBy("cust_id")
      .agg(round(sum("amount_spent"),2)
        .alias("total_spent"))

    val totalByCustomerSorted  = totalByCustomer.sort("total_spent")

    totalByCustomerSorted.show(totalByCustomer.count.toInt)
  }
}
