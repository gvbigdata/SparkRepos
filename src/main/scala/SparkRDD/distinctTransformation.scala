package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object distinctTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("mapTransformation").getOrCreate()
    val sc = spark.sparkContext
//    val n1 = sc.parallelize((1 to 10000).toList)
//    val n2 = sc.parallelize((6000 to 20000).toList)
//    val n1In2 = n1.intersection(n2).count()
    val orders = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\orders\\")
    //val order_closed = orders.filter(_.split(",")(3) == "CLOSED").take(10).foreach(println)
    val order_status = orders.map(e => e.split(",")(3)).distinct().take(10).foreach(println)
    //val order_closed = orders.map(e => e.split(",")(3).filter(e => e == "CLOSED")).take(10).foreach(println)
  }
}
