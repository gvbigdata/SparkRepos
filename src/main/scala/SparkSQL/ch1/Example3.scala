package SparkSQL.ch1
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Example3 {
  case class CancerClass(sample:Long,cThick:Int,cUSize:Int,cUShape:Int,mAdhes:Int,sECSize:Int,bNuc:Int,bChrom:Int,nNuc:Int,mitosis:Int,clas:Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.driver.host","localhost")
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("Example3").getOrCreate()
    val cancerRDD = spark.sparkContext.textFile("C:\\Anil\\Learning\\breast-cancer-wisconsin.data")
    val recordSchema = new StructType().add("Sample","long").add("cThick","integer")
      .add("uCSize","integer").add("uCShape","integer").add("mAdhes","integer")
      .add("sECSize","integer").add("bNuc","integer").add("bChrom","integer")
      .add("nNuc","integer").add("mitosis","integer").add("clas","integer")
    //print(cancerRDD.partitions.size)
    import spark.implicits._
    import org.apache.spark.sql.Row
    def row(line:List[String]):Row = {Row(line(0).toLong,line(1).toInt,line(2).toInt,line(3).toInt,line(4).toInt,
      line(5).toInt,line(6).toInt,line(7).toInt,line(8).toInt,line(9).toInt,line(10).toInt)}
    val data = cancerRDD.map(_.split(",").to[List]).map(row)
    val cancerDF = spark.createDataFrame(data,recordSchema).show(5)

  }

}
