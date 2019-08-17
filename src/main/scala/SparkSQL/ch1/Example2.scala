package SparkSQL.ch1
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class CancerClass(sample:Long,cThick:Int,cUSize:Int,cUShape:Int,mAdhes:Int,sECSize:Int,bNuc:Int,bChrom:Int,nNuc:Int,mitosis:Int,clas:Int)
object Example2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("Example2").getOrCreate()
    val cancerDS = spark.sparkContext.textFile("C:\\Anil\\Learning\\breast-cancer-wisconsin.data")
    //cancerDS.collect().foreach(println)
    import spark.implicits._
    val result = cancerDS.map({x =>
      val w = x.split(",")
      val sample = w(0).toLong
      val cThick = w(1).toInt
      val cUSize = w(2).toInt
      val cUShape = w(3).toInt
      val mAdhes = w(4).toInt
      val sECSize = w(5).toInt
      val bNuc = w(6).toInt
      val bChrom = w(7).toInt
      val nNuc = w(8).toInt
      val mitosis = w(9).toInt
      val clas = w(10).toInt
      CancerClass(sample,cThick,cUSize,cUShape,mAdhes,sECSize,bNuc,bChrom,nNuc,mitosis,clas)

    }).toDS()

    def binarize(s:Int):Int = s match {case 2 => 0 case 4 => 1}

    spark.udf.register("udfValueToCategory",(arg:Int) => binarize(arg))

    result.createOrReplaceTempView("cancerTable")
    val sqlUDF = spark.sql("select *,udfValueToCategory(clas) as Result from cancerTable").show()
  }

}
