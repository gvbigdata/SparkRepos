package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object flatMapTransformation1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("flatMapTransformation1").getOrCreate()
    val sc = spark.sparkContext
    val nyse = sc.wholeTextFiles("C:\\Anil\\Learning\\Spark\\data-master\\nyse\\nyse_data\\")
    //nyse.take(10).foreach(println)
    val nyseRDD = nyse.
      flatMap(e => e._2.split("\\r?\\n"))
    nyseRDD.take(10).foreach(println)

    val nyseRDD1 = nyse.
      flatMap(e =>{
        val a = e._2.split("\\r?\\n")
        val t = a.map(r =>{
          val rec = r.split(",")
          ((rec(1),rec(0)),rec(6).toLong)
        })
        t
      }).take(10).foreach(println)



  }

}
