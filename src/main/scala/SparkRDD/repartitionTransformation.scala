package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object repartitionTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("repartitionTransformation").getOrCreate()
    val sc = spark.sparkContext
    val l = sc.parallelize((1 to 100).toList,2)
    val repl = l.repartition(3)
  }

}
