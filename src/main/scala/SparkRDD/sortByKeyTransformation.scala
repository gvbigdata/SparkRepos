package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object sortByKeyTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("reduceByKeyTransformation").getOrCreate()
    val sc = spark.sparkContext
    val products = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\products")
    val productsMap = products.map(e =>{
      val regex =",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
      val r = e.split(regex,-1)
      (r(4).toFloat,e)
    }).sortByKey(false).take(10).foreach(println)

    val productsMap1 = products.map(e =>{
      val regex =",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
      val r = e.split(regex,-1)
      ((r(1).toInt,-r(4).toFloat),e)
    }).sortByKey().take(10).foreach(println)

  }

}
