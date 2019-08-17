package SparkSQL.ch2


import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
object Example7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host","localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("example7").getOrCreate()
    val reviewsDF = spark.read.json("C:\\Anil\\Learning\\reviews_Electronics_5.json\\Electronics_5.json")
    //print("**************************"+reviewsDF.count())
    //reviewsDF.filter("overall <3").coalesce(1).write.avro("C:/Anil/Learning/")
  }

}
