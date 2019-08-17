package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object aggregateByKeyTransformations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("reduceByKeyTransformation").getOrCreate()
    val sc = spark.sparkContext
    val order_items = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\order_items", 4)
    val orderItemMap = order_items.map(e => (e.split(",")(1).toInt, e.split(",")(4).toFloat))
    //revenue of each order and count of each order
    //input data
    //(2,199.99)
    //(2,250.0)
    //(2,129.99)

    val revenueAndCountPerOrder = orderItemMap.aggregateByKey((0.0f,0))(
      (a1:(Float,Int),a2:(Float)) => (a1._1+a2,a1._2+1),
      (f1:(Float,Int),f2:(Float,Int)) =>(f1._1+f2._1,f1._2+f2._2)
    ).take(10).foreach(println)

    //output data
    //(2,(579.98,3)
    //revenueAndCountPerOrder.saveAsTextFile("/user/hadoop/revenueAndCountPerOrder")



  }

}
