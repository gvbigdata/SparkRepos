package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
object getTopNProdctsPerDayDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("CreateDF_InferSchemaUsingReflection")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions","2")
    //val inputBaseDir = args(1)
   // val topN = args(3).toInt
    val ordersDF = spark.read.json("C:\\Anil\\Learning\\Spark\\data-master\\retail_db_json\\orders")
    //order_id,order_date
    //ordersDF.show()
    val orderItemsDF = spark.read.json("C:\\Anil\\Learning\\Spark\\data-master\\retail_db_json\\order_items")
    //order_item_order_id,order_item_product_id,order_item_Subtotal
    val productsDF = spark.read.json("C:\\Anil\\Learning\\Spark\\data-master\\retail_db_json\\products")
    //product_id,product_name

    //selection or projection of data
    ordersDF.select($"order_id",$"order_date").show()
    ordersDF.select("*").show()

    import org.apache.spark.sql.functions._
    ordersDF.select($"order_date",length($"order_date").alias("order_date_length")).show()
    ordersDF.select($"order_status").distinct().show()

    //String Manipulations
    val dual = Seq("X").toDF("dummy")//.show()
    dual.select($"dummy").show()
    //string literal using lit function
    dual.select(lit("Hello World")).show()
    dual.select(lit("Hello World").alias("dummy")).show()
    dual.select(length(lit("Hello World")).alias("dummy")).show()
    dual.select(lower(lit("Hello World")).alias("dummy")).show()
    dual.select(substring(lit("Hello World"),1,5).alias("dummy")).show()
    dual.select(instr(lit("Hello World")," ").alias("dummy")).show()
    dual.select(trim(lit("Hello World   ")).alias("dummy")).show()
    dual.select(trim(lit("Hello World000000"),"0").alias("dummy")).show()
    dual.select(lpad(lit("7"),2,"0").alias("dummy")).show()
    dual.select(concat(lit("Hello"),lit(" "),lit("World")).alias("dummy")).show()
    dual.select(initcap(lit("hello world").alias("dummy"))).show()
    dual.select(translate(lit("Hello World")," ","-").alias("dummy")).show()
    dual.select(regexp_replace(lit("2019-04-14"),"-","/").alias("date")).show

    //Date Manipulations
    dual.select(current_date().alias("dummy")).show()
    dual.select(current_timestamp().alias("dummy")).first()
    dual.select(date_add(current_date(),7).alias("dummy")).show()
    dual.select(date_sub(current_date(),7).alias("dummy")).show()
    dual.select(datediff(lit("2019-04-14"),lit("2019-01-01")).alias("dummy")).show()
    dual.select(dayofmonth(lit("2019-04-14")).alias("dummy")).show()
    dual.select(dayofyear(lit("2019-04-14")).alias("dummy")).show()
    dual.select(dayofweek(lit("2019-04-14")).alias("dummy")).show()
    dual.select(year(current_timestamp()).alias("dummy")).show()
    dual.select(quarter(current_timestamp()).alias("dummy")).show()
    dual.select(month(current_timestamp()).alias("dummy")).show()
    dual.select(hour(current_timestamp()).alias("dummy")).show()
    dual.select(minute(current_timestamp()).alias("dummy")).show()
    dual.select(second(current_timestamp()).alias("dummy")).show()
    dual.select(date_format(current_timestamp(),"dd").alias("dummy")).show()
    dual.select(date_format(current_timestamp(),"MM").alias("dummy")).show()
    dual.select(date_format(current_timestamp(),"YYYY").alias("dummy")).show()
    dual.select(date_format(current_timestamp(),"HH").alias("dummy")).show()
    dual.select(date_format(current_timestamp(),"mm").alias("dummy")).show()
    dual.select(unix_timestamp(current_timestamp()).alias("dummy")).show()
    dual.select(from_unixtime(lit(1525150135),"YYYY-MM-dd HH:mm:SS").alias("dummy")).show()

    //null and type cast functions
    dual.select(isnull(lit(null)).alias("dummy")).show()
    dual.select(lit("5").cast(IntegerType).alias("dummy")).show()

    ordersDF.select($"order_id",date_format($"order_date","YYYYMMdd").cast(IntegerType)
      .alias("order_date"),$"order_customer_id",$"order_status").show()

    //filter or where clause
    ordersDF.where("order_status = 'COMPLETE' or order_status='CLOSED'").show()
    //o
    ordersDF.where("order_status IN ('COMPLETE','CLOSED')").show()
    ordersDF.where("order_status IN ('COMPLETE','CLOSED') AND order_date LIKE '2013-08%'").show()
    ordersDF.where(col("order_status") === "COMPLETE" || col("order_status") ==="CLOSED").show()
    ordersDF.where(col("order_status").isin("COMPLETE","CLOSED")).show()
    ordersDF.where((col("order_status").isin("COMPLETE","CLOSED"))&& (col("order_date").like("2013-08%"))).show()

    //Joining the data
    ordersDF.join(orderItemsDF,$"order_id"===col("order_item_order_id")).show()
    ordersDF.join(orderItemsDF,$"order_id" === $"order_item_order_id","left").where($"order_item_order_id".isNull).show()
    //.where("order_item_order_id is null").show()

    //Aggregations
    orderItemsDF.select(sum("order_item_subtotal")).show()
    orderItemsDF.groupBy("order_item_order_id").count().show()
    //orderItemsDF.gr
    orderItemsDF.groupBy($"order_item_order_id").count().show()
    orderItemsDF.groupBy("order_item_order_id").sum("order_item_subtotal").show()
    orderItemsDF.groupBy("order_item_order_id").
      agg(sum("order_item_subtotal").alias("order_revenue")).show()

    orderItemsDF.groupBy("order_item_order_id")
      .agg(sum("order_item_subtotal").alias("order_revenue"),
        count(lit(1).alias("order_count"))).show()

    orderItemsDF.groupBy("order_item_order_id")
      .agg(sum("order_item_subtotal").alias("order_revenue"),
        count(lit(1).alias("order_count")),
        sum("order_item_quantity").alias("order_quantity")).show()

    orderItemsDF.groupBy("order_item_order_id")
      .agg(sum(col("order_item_product_price")*col("order_item_quantity")).alias("order_revenue")).show()

    //Sorting the data
    orderItemsDF.sort("order_item_product_id").show()
    orderItemsDF.sort(col("order_item_product_id").desc).show()
    orderItemsDF.sort(col("order_item_order_id"),col("order_item_subtotal").desc).show()

    //GetTopNProductsPerDay-Filtering the data
    //val ordersCompleted = ordersDF.where("order_status IN('COMPLETE','CLOSED')").show() //native sql style
    val ordersCompleted = ordersDF.where($"order_status".isin("COMPLETE","CLOSED"))//.show() //spark df style
    val ordersJoin = ordersCompleted.join(orderItemsDF,ordersCompleted("order_id") === orderItemsDF("order_item_order_id"))//.show()

    val revenueperDatePerProduct = ordersJoin
      .groupBy("order_Date","order_item_product_id")
      .agg(round(sum("order_item_subtotal"),2).alias("revenue_per_day_per_product"))//.show()

    //Analytics Functions
    val employeesRDD = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\hr_db\\employees\\part-000002.csv")
      val employeesRDD1 =employeesRDD.map(e =>{
        val a = e.split(",")
        //a.foreach(println)
        //val a = aa.map(e => e.split(",")) 24000.00

        (a(0).toInt,a(1),a(2),a(3),a(4),a(5),a(6),
          a(7).toInt,
          if(a(8)!= "null") a(8).toFloat else 0.0f,
          if(a(9)!="null")a(9).toInt else 0,
        if(a(10)!="null")a(10).toInt else 0
        )
      })

    val employees = employeesRDD1.
      toDF("employee_id","first_name","last_name","email","phone","hire_date","role","salary","commission_pct",
      "manager_id","department_id")

    import org.apache.spark.sql.expressions._
    employees.select($"*",sum("salary")
      .over(Window.partitionBy("department_id")).alias("department_Expense")).show()

    val spec = Window.partitionBy("department_id")
    employees.select($"employee_id",$"department_id",$"salary",sum("salary").over(spec).alias("department_expense")).show()
    employees.select($"employee_id",$"department_id",$"salary",avg("salary").over(spec).alias("department_expense")).show()
    employees.select($"employee_id",$"department_id",$"salary",max("salary").over(spec).alias("department_expense")).show()
    employees.select($"employee_id",$"department_id",$"salary",min("salary").over(spec).alias("department_expense")).show()
//
//    //Rank,DenseRank and Row Number functions
    val spec1 = Window.partitionBy("department_id").orderBy($"salary".desc)
    val rnk = rank().over(spec1)

    val spec2 = Window.partitionBy("department_id").orderBy(col("salary").desc)
    val rnk1 = rank().over(spec2)
    val empRanked = employees.select($"employee_id",$"department_id",$"salary",rnk1).show()
//
    val densernk = dense_rank().over(spec1)
    val empRanked1 = employees.select($"employee_id",$"department_id",$"salary",densernk).show()
//
    val rn = row_number().over(spec2).alias("rn")
    val emp_rn = employees.select($"employee_id",$"department_id",$"salary",rn).show()
//
//    //Windowing functions Lead,Lag,FirstValue and LastValue
    val spec3 = Window.partitionBy("department_id").orderBy($"salary".desc)
    val fv = first("salary").over(spec3)
    val empfv = employees.select($"employee_id",$"department_id",$"salary",fv).show()

    val fv1 = first("salary").over(spec3).alias("first_value")
    val empfv1 = employees.select($"employee_id",$"department_id",$"salary",fv1).show()
//
    val lv = last("salary").over(spec3).alias("last_value")
    val emplv = employees.select($"employee_id",$"department_id",$"salary",lv).show()

    println("**********lead******************")
//
    val ld = lead("salary",1).over(spec3).alias("next_sal")
    val empld = employees.select($"employee_id",$"department_id",$"salary",ld).show()

    val neid = lead("employee_id",1).over(spec3).alias("next_employee_id")
    val empld1 = employees.select($"employee_id",$"department_id",$"salary",ld,neid).show()

    println("**********lag******************")
    val pe = lag("employee_id",1).over(spec3).alias("prev_employee_id")
    val ps = lag("salary",1).over(spec3).alias("prev_employee_sal")
    val emplg = employees.select($"employee_id",$"department_id",$"salary",ps,pe).show()
//
    revenueperDatePerProduct.show()
    val spec4 = Window.partitionBy("order_Date").orderBy($"revenue_per_day_per_product".desc)

    val sal_rnk = rank().over(spec4)


    //val topNProductIdsPerDay = revenueperDatePerProduct.select($"*",sal_rnk).where("sal_rnk<=3").show()
//
//    topNProductIdsPerDay.sort("order_date","rnk").write.json(args(4))










  }

}
