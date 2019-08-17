package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    //case class orders(order_id:Int,order_date:String,order_customer_id:Int,order_status:String)
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master(args(0)).appName("sparkFileExample").getOrCreate()
    val data = spark.sparkContext.textFile(args(1))
    val words = data.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_+_)
    words.saveAsTextFile(args(2))
  }
}

