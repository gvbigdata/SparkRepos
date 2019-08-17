package SparkSQL.ch2
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Example6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host","localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("Example6").getOrCreate()
    val reviewsDF = spark.read.json("C:\\Anil\\Learning\\reviews_Electronics_5.json\\Electronics_5.json")
    //reviewsDF.printSchema()
    reviewsDF.createOrReplaceTempView("reviewsTable")
    val selectedDF = spark.sql("select asin,overall,reviewTime,reviewerID,reviewerName from reviewsTable where overall >=3").show()
    import spark.implicits._
    val selectedJSONArrayElementDF = reviewsDF.select("asin","overall","helpful").where($"helpful".getItem(0)<3)
    selectedJSONArrayElementDF.show()
  }

}
