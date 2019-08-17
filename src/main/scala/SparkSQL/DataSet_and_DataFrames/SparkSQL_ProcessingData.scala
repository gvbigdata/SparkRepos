package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object SparkSQL_ProcessingData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().master(args(0)).config(conf = conf).appName("SparkSQL_ProcessingData").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val orders = spark.read.json(args(1))
    orders.createOrReplaceTempView("orders_table")
    spark.sql("show tables").show()
    val result = spark.sql("select * from orders_table").show()
    val result1 = spark.sql("select order_status,count(1) order_count from orders_table group by order_status").show()

    spark.sql("use default")
    spark.sql("show tables").show()

    //Functions is Spark SQL
    spark.sql("show functions").show()
    spark.sql("select current_date()").show()
    spark.sql("describe function current_date").collect().foreach(println)

    //GetRevenuePerOrder using SparkSQL
    val orderItems = spark.read.json(args(2))
    orderItems.createOrReplaceTempView("order_items")
    spark.sql("select order_item_order_id, sum(order_item_subtotal) revenue_per_order " +
      "from order_items group by order_item_order_id").show()

    //SparkSQL-Hive Tables
    spark.sql("show databases").collect().foreach(println)
    //spark.sql("use retail_db").collect().foreach(println)
    //spark.sql("select * from order_items").collect().foreach(println)

    //GetTopNProductsPerDay using SparkSQL
    spark.conf.set("spark.sql.shuffle.partitions","2")
    val inputBaseDir = args(3)
    val ordersDF = spark.read.json(inputBaseDir+"/orders")
    ordersDF.createOrReplaceTempView("orders")

    val ordersItemsDF = spark.read.json(inputBaseDir+"/order_items")
    ordersItemsDF.createOrReplaceTempView("order_items")

    val productsDF = spark.read.json(inputBaseDir+"/products")
    productsDF.createOrReplaceTempView("products")

    spark.sql("show tables").show()
    spark.sql("select * from orders").show()
    spark.sql("select * from order_items").show()
    spark.sql("select * from products").show()

    //Selection and Project in SparkSQL
    spark.sql("describe orders").show()
    spark.sql("select order_item_order_id,order_item_subtotal from order_items").show()
    spark.sql("select order_item_product_price*order_item_quantity as order_revenue from order_items").show()

    //String Manipulation Functions - Case Conversions
    //aggregate and row level functions
    //row level functions - string manipulation and date manipulation functions.
    //String manipulation - case conversion,trim,padding,length
    spark.sql("describe function initcap").collect().foreach(println)
    spark.sql("select initcap('hello world')").collect().foreach(println)

    //ltrim,rtrim,trim
    spark.sql("describe function trim").collect().foreach(println)
    spark.sql("select ltrim('0','090')").show()
    spark.sql("select trim(LEADING '0' FROM '090')").show()
    spark.sql("select substr('1234567890',1,3)").show()
    spark.sql("select split('234 street name,city,state',',')").show()

    //String Concatenation
    spark.sql("select concat(2017,1)").show()
    spark.sql("select concat(2017 || 05 || 01)").show()

    spark.sql("desribe function lpad").collect().foreach(println)
    spark.sql("select lpad(10,2,0),lpad(1,2,0)").show()
    spark.sql("select concat(2017, '-', lpad(5,2,0), '-', lpad(1,2,0))").show()










  }
}
