package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object catalystOptimizer {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("CreateDF_InferSchemaUsingReflection")
      .getOrCreate()

//    userid,email,location
//    1,abc@gmail.com.com,hyd
//    2,def@gmail.com,bang
//    3,ghi@gmail.com,us
//    4,jkl@gmail.com,uk
//    5,mno@gmail.com,rus

    val userData = spark.read.option("header",true).option("delimiter",",").option("inferSchema",true).
      csv("C:\\Anil\\Learning\\user.csv")


//    tid,pid,userid,amount,itemdesc
//    1,1,1,100,bat
//    2,2,1,200,ball
//    3,3,1,60,gloves
//    4,1,2,1,bat
//    5,4,2,60,gaurd
//    6,1,2,90,bat
//    7,5,4,9,stumps
//    8,3,4,3,bells
//    9,2,5,7,mat
//    10,2,5,9,pads

    val purData = spark.read.option("header",true).option("delimiter",",").option("inferSchema",true).
      csv("C:\\Anil\\Learning\\purchage.csv")

    val joinedOpt = purData.join(userData,Seq("userid"),"left").select("pid","location").
      filter("amount>60").select("location")

    joinedOpt.explain(true)
  }

}
