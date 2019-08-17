package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object dfToDS {
  case class order(order_id:Int,order_date:String,order_customer_id:Int,order_status:String)
  def main(args: Array[String]): Unit = {
    //case class orders(order_id:Int,order_date:String,order_customer_id:Int,order_status:String)
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master(args(0)).appName("dfToDS").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    //val orders = spark.read.csv(args(1))
    //orders.show()
    val sc = spark.sparkContext
    val orders = sc.textFile(args(1))
    val ordersDF = orders.map(s => {
      val a = s.split(",")
      order(a(0).toInt,a(1),a(2).toInt,a(3))
    }
    ).toDF()
    // The columns of a row in the result can be accessed by field index
    //ordersDF.map(x => "Order_id: "+x(0)).show()

    // or by field name
    //ordersDF.map(x => "Name: " + x.getAs[String]("order_id")).show()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    //ordersDF.map(x => x.getValuesMap[Any](List("order_id", "order_item_subtotal"))).collect()



    val ordersDS = ordersDF.as[order]
    ordersDS.show()

  }


}
