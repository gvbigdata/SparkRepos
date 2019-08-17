package SparkSQL.ch1
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Example1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf = conf).master("local").appName("Example1").getOrCreate()
    System.setProperty("hadoop.home.dir","C:\\Anil\\Learning\\hadoop\\")
    val recordSchema = new StructType().add("Sample","long").add("cThick","integer")
      .add("uCSize","integer").add("uCShape","integer").add("mAdhes","integer")
      .add("sECSize","integer").add("bNuc","integer").add("bChrom","integer")
      .add("nNuc","integer").add("mitosis","integer").add("clas","integer")

    val df = spark.read.format("csv").option("header",false).schema(recordSchema).load("C:\\Anil\\Learning\\breast-cancer-wisconsin.data")
    df.show(2)
    df.createOrReplaceTempView("cancerTable")
    val sqlDF = spark.sql("select  sample,bNuc from cancerTable").show(5)
    print(spark.catalog.currentDatabase)
    print(spark.catalog.isCached("cancerTable"))
    print(spark.catalog.cacheTable("cancerTable"))
    print(spark.catalog.isCached("cancerTable"))
    spark.catalog.clearCache()
    print(spark.catalog.isCached("cancerTable"))
    print(spark.catalog.listDatabases().show())
    print(spark.catalog.listTables("default").show())
    spark.catalog.dropTempView("cancerTable")
    print(spark.catalog.listTables("default").show())
  }

}
