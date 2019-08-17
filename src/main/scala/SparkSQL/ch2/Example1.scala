package SparkSQL.ch2
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.util.Properties
object Example1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("Example1").getOrCreate()
    val inFileRDD = spark.read.format("csv").option("header",true).option("inferschema",true).load("C:\\Anil\\Learning\\OnlineRetailData.txt")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val ts = unix_timestamp($"InvoiceDate","dd/MM/yy HH:mm").cast("timestamp")
    val r2DF = inFileRDD.withColumn("ts",ts)
    r2DF.createOrReplaceTempView("retailTable")
    val r3DF = spark.sql("select * from retailTable where ts<'2011-12-01'")
    val r4DF = spark.sql("select * from retailTable where ts>='2011-12-01'")
    val selectData = r4DF.select("InvoiceNo","StockCode","Description","Quantity","UnitPrice","CustomerID","Country","ts")
    val writeData = selectData.withColumnRenamed("ts","InvoiceDate")
    //writeData.show(5)

    val dbUrl = "jdbc:mysql://localhost:3306/retailDB"
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    writeData.write.mode("append").jdbc(dbUrl,"transactions",prop)
    print("***************data sucessfully copied to transactions table*******************")

//    val selectData1 = r3DF.select("InvoiceNo","StockCode","Description","Quantity","UnitPrice","CustomerID","Country","ts")
//    val writeData1 = selectData1.withColumnRenamed("ts","InvoiceDate")
//    writeData1.select("*").write.format("json").save("C:\\Anil\\Learning\\r3DF")


  }

}
