package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object joinTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("mapTransformation").getOrCreate()
    val sc = spark.sparkContext
    val orders = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\orders\\")
    val order_items = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\order_items")
    val ordersMap = orders.map(e => (e.split(",")(0).toInt,e))
    val orderItemsMap = order_items.map(e =>(e.split(",")(1).toInt,e))
    ordersMap.join(orderItemsMap).take(10).foreach(println)

    val orderLeftOuterJoin = ordersMap.leftOuterJoin(orderItemsMap)//.take(10).foreach(println)
    orderLeftOuterJoin.filter(e => e._2._2==None).map(e => e._2._1).take(10).foreach(println)



  }

}
