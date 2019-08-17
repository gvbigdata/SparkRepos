package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object mapTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().master("local").config(conf=conf).appName("mapTransformation").getOrCreate()
    val sc = spark.sparkContext
    val orders = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\orders\\")
    orders.first()
    val orderDates = orders.map(e => e.split(",")(1))
    orderDates.take(10).foreach(println)
    println(orders.count())
    println(orderDates.count())
    val orderDate_YYYYMMDD = orders.map(e=>e.split(",")(1).substring(0,10).replace("-","").toInt)
    orderDate_YYYYMMDD.take(10).foreach(println)
    val orderDates_pair = orders.map(e=>(e.split(",")(1).substring(0,10).replace("-","").toInt,1)).reduceByKey(_+_)
    orderDates_pair.take(10).foreach(println)
  }

}
