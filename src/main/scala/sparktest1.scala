import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object sparktest1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("sparktest1").getOrCreate()
    var counter = 0
    val data = List(1,2,3,4,5)
    val rdd = spark.sparkContext.parallelize(data)
    rdd.foreach(x => counter+= x)
    println("Counter value: " + counter)
  }

}
