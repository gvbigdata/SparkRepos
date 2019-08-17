package SparkRDD
/*
Problem Statement:Get top n products per day by revenue
Consider only COMPLETE and CLOSED orders.
Output: order_date,product_name,revenue
 */
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object GetTopNProductsPerDay {
  def getTopNProductsForDay(productsForDay:(String,List[(Int,Float)]),topN:Int)={
    productsForDay._2.sortBy(k => -k._2).take(5).map(e =>(productsForDay._1,e))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf = conf).master(args(0)).appName("GetTopNProductsPerDay").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //reading the data from file system or database
    //read orders,order_items and products
    val inputBaseDir = args(1)
    val orders = sc.textFile(inputBaseDir+"/orders")
    val orderItems = sc.textFile(inputBaseDir+"/order_items")
    val products = sc.textFile(inputBaseDir+"/products")

    //Row level transformations
    //orders -> filter and apply map transformations
    val ordersFiltered = orders.filter(e => e.split(",")(3) =="COMPLETE" ||e.split(",")(3)=="CLOSED")
    //val ordersFiltered = orders.filter(e => List("COMPLETE","CLOSED").contains(e.split(",")(3)))
    val ordersMap = ordersFiltered.map(e => (e.split(",")(0).toInt,e.split(",")(1)))//.take(10).foreach(println)

    //order_items->order_id,productid,order_item_subtotal(1,(957,299.98))
    val orderItemsMap = orderItems.map(e =>( e.split(",")(1).toInt,(e.split(",")(2).toInt,e.split(",")(4).toFloat)))//.
      //take(10).foreach(println)

    //reading the data products(productid,productname)
    val regex =",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    val productsMap = products.map(e =>(e.split(regex,-1)(0).toInt,e.split(regex,-1)(2)))//.
      //take(10).foreach(println)

    //orders join order_items
    val ordersJoin = ordersMap.join(orderItemsMap).
      map(e => ((e._2._1,e._2._2._1),e._2._2._2))//.take(10).foreach(println)

    //compute revenue per day per product
    val revenuePerDayPerProdcutId = ordersJoin.
      reduceByKey(_+_)
    val productIdAndRevenuePerDay = revenuePerDayPerProdcutId.
      map(e => (e._1._1,(e._1._2,e._2)))

    //get top n products per day
    val productsPerDay = productIdAndRevenuePerDay.groupByKey()
    val topNProductsPerDay =productsPerDay.flatMap(e =>getTopNProductsForDay((e._1,e._2.toList),args(3).toInt)).
      map(e => (e._2._1,(e._1,e._2._2)))
      //take(10).foreach(println)

    val topNProductsPerDay1 = productsMap.join(topNProductsPerDay).
      map(e => ((e._2._2._1,-e._2._2._2),e._2._2._1+"\t"+e._2._1+"\t"+e._2._2._2)).sortByKey().
      //take(10).foreach(println)
      map(e => e._2)//.take(10).foreach(println)

    topNProductsPerDay1.saveAsObjectFile(args(2))







  }

}
