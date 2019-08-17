package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
object CreateDF_ApplyingSchemaProgrammaticallyUsingTypes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("CreateDF_InferSchemaUsingReflection")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val s = "order_id:Int,order_date:String,order_customer_id:Int,order_status:String"
    val fileds = s.split(",").map(e => {
        val f = e.split(":")(0)
        val t = if(e.split(":")(1) == "Int") IntegerType else StringType
        StructField(f,t,nullable = false)
      })
    val schema = StructType(fileds)


    val orderRDD = sc.textFile("C:\\Anil\\Learning\\Spark\\data-master\\retail_db\\orders\\part-00000")
    .map(e =>{
        val a = e.split(",")
        Row(a(0).toInt,a(1),a(2).toInt,a(3))
      })

    val orderDF = spark.createDataFrame(orderRDD,schema)
    orderDF.show()


  }

}
