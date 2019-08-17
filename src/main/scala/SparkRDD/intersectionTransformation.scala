package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object intersectionTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("mapTransformation").getOrCreate()
    val sc = spark.sparkContext
    val n1 = sc.parallelize((1 to 10000).toList)
    val n2 = sc.parallelize((6000 to 20000).toList)
    val n1In2 = n1.intersection(n2).count()//it will not allow duplicates

  }

}
