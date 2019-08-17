package SparkSQL.DataSet_and_DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object firstDataFrame {
  case class employee(eno:Int,ename:String,sal:Double,gendar:String,dno:Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("firstDataFrame").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
//    val empRDD = sc.textFile("C:\\Users\\Anil_Kanaparthi\\Desktop\\emp.csv")
//    val e = empRDD.map(x =>{
//      val w = x.split(",")
//      val eno = w(0).toInt
//      val ename = w(1)
//      val sal = w(2).toDouble
//      val gendar = w(3)
//      val dno = w(4).toInt
//      employee(eno,ename,sal,gendar,dno)
//    }).toDF()
//    e.createOrReplaceTempView("employee")
//    val result = spark.sql("select dno,avg(sal) from employee group by dno").explain()
    val empDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:\\Users\\Anil_Kanaparthi\\Desktop\\emp.csv").show()
  }

}
