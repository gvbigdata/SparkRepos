package SparkRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetRevenueForOrderUsingParallelize {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//    val spark = SparkSession.builder().config(conf = conf).master("local").appName("GetRevenueForOrderUsingParallelize").getOrCreate()
//    val orderItemsPath = args(0)
//    val orderId = args(1)
//
//    val orderItems = scala.io.Source.fromFile(orderItemsPath).getLines().toList
//    //orderItems.take(10).foreach(println)
//    val orderItemsFiltered = orderItems.filter(f => f.split(",")(1) == orderId)
//    // orderItemsFiltered.foreach(println)
//    val orderItemSubtotals = orderItemsFiltered.map(m => m.split(",")(4).toFloat)
//    //orderItemSubtotals.foreach(println)
//    val orderRevenue = orderItemSubtotals.reduce(_+_)
//    println("Order revenue for order id "+ orderId +" is "+ orderRevenue)

    val orderItemsPath = args(1)
    val orderId = args(2)
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master(args(0)).appName("Get revenue for order_id "+orderId+" using parallelize").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val orderItemsList = scala.io.Source.fromFile(orderItemsPath).getLines().toList
    val orderItems = sc.parallelize(orderItemsList)
    val orderItemsFiltered = orderItems.filter(f => f.split(",")(1) == orderId)
    val orderItemSubtotals = orderItemsFiltered.map(m => m.split(",")(4).toFloat)
    val orderRevenue = orderItemSubtotals.reduce(_+_)
    println("Order revenue for order id "+ orderId +" is "+ orderRevenue)















  }

}
