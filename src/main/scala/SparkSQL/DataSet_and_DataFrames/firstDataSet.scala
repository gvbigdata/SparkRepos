package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object firstDataSet {
  case class Order(order_id:Int,order_date:String,order_customer_id:Int,order_status:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf=conf).master(args(0)).appName("firstDataset").getOrCreate()
    val sc = spark.sparkContext
    val orders = sc.textFile(args(1))
    val ordersMap = orders.map(s =>{
      val a = s.split(",")
      Order(a(0).toInt,a(1),a(2).toInt,a(3))
    }
    )
    import spark.implicits._
    val ordersDS = ordersMap.toDS()
    println(ordersDS.printSchema())
    ordersDS.show()

    //RDD Approch
    ordersDS.filter(o => o.order_status=="COMPLETE").show()

    //DataFrame approch
    ordersDS.filter($"order_status" === "COMPLETE").show()
  }

}
