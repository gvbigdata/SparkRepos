package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object CreateDF_StructType {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("CreateDF_StructType").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    val data = Seq(Row(1, "a"),Row(5, "z"))

    val schema = StructType(
      List(
        StructField("num", IntegerType, true),
        StructField("letter", StringType, true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    df.show()
  }

}
