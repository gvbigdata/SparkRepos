package SparkSQL.ch1
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class RestClass(name:String,street:String,city:String,phone:String,cuisine:String)
object Example4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host","localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("Example4").getOrCreate()
    val rest1DS = spark.sparkContext.textFile("C:\\Anil\\Learning\\restaurant.tar\\restaurant\\original\\fodors.csv")
    //rest1DS.collect().foreach(println)
    import spark.implicits._
    val rest2DS = rest1DS.map({x =>
      val w = x.split(",")
      val name = w(0)
      val street = w(1)
      val city = w(2)
      val phone = w(3)
      val cuisine = w(4)
      RestClass(name,street,city,phone,cuisine)
    })

    def formatPhoneNo(s:String):String= s match{
      case s if s.contains("/") => s.replaceAll("/","-").replaceAll("- ","-").replaceAll("--","-")
      case _ => s

    }

    spark.udf.register("udfValueToCategory",(arg:String) => formatPhoneNo(arg))

    val rest2Df = rest2DS.toDF()
    rest2Df.createOrReplaceTempView("rest3df")
    val result = spark.sql("select *,udfValueToCategory(phone) from rest3df").show(3)

  }

}
