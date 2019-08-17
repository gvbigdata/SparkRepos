import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
object sparktest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("sparktest").getOrCreate()
    val l = List(1,2,3,4,5)
    val data = spark.sparkContext.parallelize(l)
    val result = data.map(x => x+10).foreach(println)

  }

}
