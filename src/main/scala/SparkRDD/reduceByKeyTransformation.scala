package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object reduceByKeyTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("reduceByKeyTransformation").getOrCreate()
    val sc = spark.sparkContext
    val order_items =sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\order_items",4)
    val orderItemMap = order_items.map(e =>(e.split(",")(1).toInt,e.split(",")(4).toFloat))//.collect().foreach(println)
   val revenuePerOrder = orderItemMap.reduceByKey(_+_,3).take(10).foreach(println)
   // val maxRevenuePerOrder = orderItemMap.reduceByKey((x,y) => if(x>y) x else y ).take(10).foreach(println)

//    val data = List("spark is powerful engine","spark replaces mapreduce","i am learning spark")
//    val rdd1 = sc.parallelize(data)
//    val result = rdd1.flatMap(e => e.split(" ")).map(e => (e,1)).reduceByKey((x,y) => x+y).collect().foreach(println)



  }

}
