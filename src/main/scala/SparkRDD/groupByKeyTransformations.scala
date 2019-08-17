package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object groupByKeyTransformations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("groupByKeyTransformations").getOrCreate()
    val sc = spark.sparkContext
    val order_items =sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\order_items",4)
    val orderItemMap = order_items.map(e =>(e.split(",")(1).toInt,e.split(",")(4).toFloat))

    val orderItemsGroupByOrderId = orderItemMap.groupByKey(3).filter(e => e._1==57436).collect().foreach(println)
    //val t = orderItemsGroupByOrderId.filter(e => e._1 == 2).first()
//    val revenuePerOrder = orderItemsGroupByOrderId.map(e => (e._1,e._2.sum)).take(10).foreach(println)
//    val topSubtotalPerOrder = orderItemsGroupByOrderId.map(e => (e._1,e._2.toList.sortBy(r => -r).toList(0)))//.take(10).foreach(println)
//    println(topSubtotalPerOrder.filter(e => e._1 ==2 ).first)
//
//    val dataSorted = orderItemsGroupByOrderId.
//      flatMap(e =>{
//        val sortedSubTotals = e._2.toList.sortBy(k => -k)
//        sortedSubTotals.map(r => (e._1,r))
//      })//.take(10).foreach(println)
//    println(dataSorted.filter(e => e._1 ==29270).collect.foreach(println))
  }
}
