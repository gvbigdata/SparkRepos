package SparkSQL
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StockDataAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("StockDataAnalysis").getOrCreate()
    val raw_stock_data=spark.sparkContext.textFile("C:\\Anil\\Learning\\Data\\Stocks\\*")
    println("**************************************"+raw_stock_data.count())
  }

}
