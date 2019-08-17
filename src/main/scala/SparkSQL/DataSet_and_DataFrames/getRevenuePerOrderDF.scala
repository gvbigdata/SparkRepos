package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object getRevenuePerOrderDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master(args(0)).config(conf = conf).appName("getRevenuePerOrderDF").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions","2")
    val orderItems = spark.read.option("mode","PERMISSIVE").json(args(1))
    //orderItems.show()
    val revenuePerOrder =orderItems.groupBy($"order_item_order_id").sum("order_item_subtotal")
    revenuePerOrder.write.json(args(2))
  }

}
