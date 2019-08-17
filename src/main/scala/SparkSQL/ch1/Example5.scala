package SparkSQL.ch1
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object Example5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("Example5").getOrCreate()
    val users = spark.read.format("csv").option("header", true).option("inferschema", true).load("C:\\Anil\\Learning\\user.csv")
    val purchase = spark.read.format("csv").option("header", true).option("inferschema", true).load("C:\\Anil\\Learning\\purchage.csv")
    val userJndPurc = purchase.join(users, Seq("userid"), "left").select("pid", "location").filter("amount>60")
      .select("location")
    userJndPurc.explain(true)

  }
}
