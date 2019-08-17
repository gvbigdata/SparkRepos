package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.sql.{SaveMode, SparkSession}

object JdbcRDDExample{
  def main(args: Array[String]){
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("JdbcRDDExample")
      .getOrCreate()

    val prop=new java.util.Properties()
    prop.put("user","root")
    prop.put("password","root")
    val url="jdbc:mysql://localhost:3306/retaildb"

    val df=spark.read.jdbc(url,"transactions",prop)
    df.show()
    df.write.mode(SaveMode.Append).jdbc(url,"transactions1",prop)

  }
}



