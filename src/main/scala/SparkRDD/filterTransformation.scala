package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object filterTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("mapTransformation").getOrCreate()
    val sc = spark.sparkContext
    val orders = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\orders\\")
    val completedOrder = orders.filter(e => e.split(",")(3) == "COMPLETE" || e.split(",")(3) == "CLOSED")
    completedOrder.take(10).foreach(println)
    val orders201307 = orders.filter(e => e.split(",")(1).startsWith("2013-07")).take(10).foreach(println)
    println("**********************************")
    val orders201307Completed = orders.filter(e =>{
      e.split(",")(1).startsWith("2013-07") &&(e.split(",")(3) == "COMPLETE" || e.split(",")(3) == "CLOSED")

    }).count()


  }
}
