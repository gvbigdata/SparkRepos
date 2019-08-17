package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object unionTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("mapTransformation").getOrCreate()
    val sc = spark.sparkContext
    val n1 = sc.parallelize((1 to 10000).toList)
    val n2 = sc.parallelize((6000 to 20000).toList)
    val n1Un2 = n1.union(n2).count()//union allows duplicate of records



  }

}
