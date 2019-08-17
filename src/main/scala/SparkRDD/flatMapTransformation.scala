package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object flatMapTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("flatMapTransformation").getOrCreate()
    val sc = spark.sparkContext
    val lines = List("Hello World","how are you","are you interested","in writing","simple word count program","using spark",
    "to demonstrate","flatmap")
    val linesRDD = sc.parallelize(lines)
    val linesMap = linesRDD.map(e => e.split(" ")).count()
    val words = linesRDD.flatMap(e => e.split(" ")).collect.foreach(println)

  }
}
