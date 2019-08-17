package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object CreateDF_InferSchemaUsingReflection {
  case class order(order_id:Int,order_date:String,order_customer_id:Int,order_status:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master(args(0)).appName("CreateDF_InferSchemaUsingReflection")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val orders = sc.textFile(args(1))
    val ordersDF = orders.map(s => {
      val a = s.split(",")
      order(a(0).toInt,a(1),a(2).toInt,a(3))
    }
    ).toDF()
    //.todF(order_id,order_date,order_customer_id,order_status)

  }

}
