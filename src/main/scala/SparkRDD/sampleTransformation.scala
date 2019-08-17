package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object sampleTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("mapTransformation").getOrCreate()
    val sc = spark.sparkContext
    val orders = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\orders\\")
    val ordersSample = orders.sample(false,0.1,100).count()

  }


}
