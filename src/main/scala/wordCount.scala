import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object wordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("wordCount").getOrCreate()
    val rawData = List("Hello Good Morning","Today Kata Topic is Bigdata",
      "Bigdata is very useful for large datasets",
      "Hadoop and Spark for store and processing the data")
    val sc = spark.sparkContext
    val rdd = sc.parallelize(rawData)
    val fmap = rdd.flatMap(x => x.split(" "))//.collect().foreach(println)
    val mapRDD = fmap.map(x => (x,1))//.collect().foreach(println)
    val result = mapRDD.reduceByKey(_+_).collect().foreach(println)
    //result.saveAsTextFile("C:\\Anil\\Learning\\Kata_23042019\\wc_opt")


  }
}
